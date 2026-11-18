package ghpulse.flink.live

import ghpulse.core.cli.{Args, JobConfig}
import ghpulse.core.domain.GitHubEvent
import ghpulse.flink.{EventStream, RankedRepo, TrendingRepos}
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala._

import java.time.Duration

/** Options only the live job has; everything shared with the batch job stays in [[JobConfig]]. */
final case class LiveOptions(
    stateFile: String,
    slideMillis: Long,
    maxBursts: Int,
    trendingIncludesBots: Boolean,
    follow: Boolean
)

object LiveOptions {

  val usage: String =
    """Live options:
      |  --state-file <path>   where the dashboard snapshot is written     (default: state.json)
      |  --slide <minutes>     how often the trending board refreshes      (default: 2)
      |  --max-bursts <n>      bursts kept in the feed                     (default: 50)
      |  --trending-bots       let [bot] accounts count towards trending   (default: off)
      |  --once                read --input once and stop instead of following it
      |""".stripMargin

  def fromArgs(args: Args): LiveOptions =
    LiveOptions(
      stateFile = args.str("state-file", "state.json"),
      slideMillis = args.long("slide", 2) * 60000L,
      maxBursts = args.int("max-bursts", 50),
      trendingIncludesBots = args.bool("trending-bots", default = false),
      follow = !args.bool("once", default = false)
    )
}

/** The public dashboard's job: follow the poller's output directory, keep a live burst feed and a
  * trending leaderboard, and write both to one JSON snapshot for a static web server to serve.
  *
  * ```
  * java -jar gh-pulse-live.jar --input /var/lib/gh-pulse/incoming \
  *   --state-file /var/lib/gh-pulse/public/state.json
  * ```
  */
object LiveDashboardJob {

  /** Defaults that differ from the batch job's, because the input differs: the public Events API is
    * a sample of the firehose, so a burst needs fewer events over a longer window to mean the same
    * thing. Anything on the command line still overrides these.
    */
  val liveDefaults: Seq[String] = Seq(
    "top" -> "15",
    "window" -> "60",
    "burst-events" -> "5",
    "burst-window" -> "10",
    "burst-cooldown" -> "15",
    "out-of-orderness" -> "300"
  ).flatMap { case (flag, value) => Seq(s"--$flag", value) }

  /** How often to look for new files from the poller. */
  val DiscoverEvery: Duration = Duration.ofSeconds(5)

  /** How long to collect changes before rewriting the snapshot. */
  val FlushEveryMillis: Long = 2000L

  def main(argv: Array[String]): Unit = {
    // Later flags win in Args.parse, so putting the defaults first lets the command line override.
    val all = liveDefaults ++ argv.toSeq
    JobConfig.fromArgs(all) match {
      case Left(error) =>
        Console.err.println(s"$error\n${LiveOptions.usage}")
        sys.exit(2)
      case Right(config) =>
        val live = LiveOptions.fromArgs(Args.parse(all))
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // A few hundred events a minute: one slot is plenty.
        env.setParallelism(1)
        wire(source(env, config, live), config, live)
        env.execute("gh-pulse-live")
    }
  }

  /** Follows the input directory forever, or with `--once` reads it a single time. */
  private def source(
      env: StreamExecutionEnvironment,
      config: JobConfig,
      live: LiveOptions
  ): DataStream[GitHubEvent] = {
    val lateness = Duration.ofMillis(config.maxOutOfOrdernessMillis)
    if (live.follow)
      EventStream.followDirectory(env, config.input, config.decoder, lateness, DiscoverEvery)
    else EventStream.fromPath(env, config.input, config.decoder, lateness)
  }

  /** Everything after the source, separated out so tests can feed it a fixture stream. */
  def wire(events: DataStream[GitHubEvent], config: JobConfig, live: LiveOptions): Unit = {
    val bursts = LiveBursts(events, config.burst)

    // Bots stay in the burst feed — that is the point of it — but a repository whose only activity
    // is a CI bot pushing every minute is not "blowing up", so they skip the leaderboard.
    val forTrending =
      if (live.trendingIncludesBots) events
      else events.filter(e => !LiveBurst.isBot(e.actor.login)).name("drop-bots")
    val trending =
      TrendingRepos(forTrending, config.windowMillis, live.slideMillis, config.topK, config.weights)

    val settings = DashboardSettings(
      burstEvents = config.burst.minEvents,
      burstWindowMillis = config.burst.windowMillis,
      trendingWindowMillis = config.windowMillis,
      trendingSlideMillis = live.slideMillis,
      trendingIncludesBots = live.trendingIncludesBots
    )

    // Both streams go to one writer instance (constant key, parallelism 1), so there is exactly
    // one process writing the snapshot file.
    bursts
      .connect(trending)
      .keyBy((_: LiveBurst) => 0, (_: RankedRepo) => 0)
      .process(new DashboardWriter(live.stateFile, settings, live.maxBursts, FlushEveryMillis))
      .name("dashboard")
      .setParallelism(1)
      .addSink(new DiscardingSink[String]) // Flink only runs operators that lead to a sink
      .name("done")
  }
}
