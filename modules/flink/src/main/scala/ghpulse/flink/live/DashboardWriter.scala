package ghpulse.flink.live

import ghpulse.flink.RankedRepo
import io.circe.Json
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

/** What the dashboard knows about itself, echoed into the snapshot so the page can label its
  * numbers honestly ("5+ events in 10 min") instead of hard-coding them.
  */
final case class DashboardSettings(
    burstEvents: Int,
    burstWindowMillis: Long,
    trendingWindowMillis: Long,
    trendingSlideMillis: Long,
    trendingIncludesBots: Boolean
)

/** Everything the page shows, and its JSON form. Pure, so it is tested without Flink.
  *
  * @param top
  *   the leaderboard of the newest closed window, by rank.
  * @param previous
  *   the window before it, by repository, to show rank and score movement.
  */
final case class Dashboard(
    bursts: Vector[LiveBurst],
    windowStart: Long,
    windowEnd: Long,
    top: Vector[RankedRepo],
    previousWindowEnd: Long,
    previous: Map[String, RankedRepo]
) {

  /** Newest burst first; the same burst replayed after a restart is not listed twice. */
  def withBurst(b: LiveBurst, keep: Int): Dashboard =
    if (bursts.exists(x => x.actor == b.actor && x.startedAt == b.startedAt)) this
    else copy(bursts = (b +: bursts).sortBy(x => -x.endedAt).take(keep))

  /** Adds one leaderboard row. Rows of a window arrive together; the first row of a newer window
    * starts a new board, and the current one becomes `previous`.
    */
  def withRanked(r: RankedRepo): Dashboard =
    if (r.windowEnd > windowEnd)
      copy(
        windowStart = r.windowStart,
        windowEnd = r.windowEnd,
        top = Vector(r),
        previousWindowEnd = windowEnd,
        previous = top.map(x => x.repo -> x).toMap
      )
    else if (r.windowEnd == windowEnd) copy(top = (top :+ r).sortBy(_.rank))
    else this // older than the board on screen

  def toJson(settings: DashboardSettings, generatedAt: Long): Json =
    Json.obj(
      "generatedAt" -> Json.fromLong(generatedAt),
      "settings" -> Json.obj(
        "burstEvents" -> Json.fromInt(settings.burstEvents),
        "burstWindowMinutes" -> Json.fromLong(settings.burstWindowMillis / 60000L),
        "trendingWindowMinutes" -> Json.fromLong(settings.trendingWindowMillis / 60000L),
        "trendingSlideMinutes" -> Json.fromLong(settings.trendingSlideMillis / 60000L),
        "trendingIncludesBots" -> Json.fromBoolean(settings.trendingIncludesBots)
      ),
      "trending" -> Json.obj(
        "windowStart" -> Json.fromLong(windowStart),
        "windowEnd" -> Json.fromLong(windowEnd),
        "previousWindowEnd" ->
          (if (previous.nonEmpty) Json.fromLong(previousWindowEnd) else Json.Null),
        "repos" -> Json.fromValues(top.map(repoJson))
      ),
      "bursts" -> Json.fromValues(bursts.map(burstJson))
    )

  private def repoJson(r: RankedRepo): Json = {
    val before = previous.get(r.repo)
    Json.obj(
      "rank" -> Json.fromInt(r.rank),
      "repo" -> Json.fromString(r.repo),
      "score" -> Json.fromDoubleOrNull(r.score),
      "previousRank" -> before.fold(Json.Null)(b => Json.fromInt(b.rank)),
      "previousScore" -> before.fold(Json.Null)(b => Json.fromDoubleOrNull(b.score)),
      // "New" only means something once there is a previous board to be new relative to.
      "isNew" -> Json.fromBoolean(previous.nonEmpty && before.isEmpty),
      "events" -> Json.fromLong(r.stats.events),
      "pushes" -> Json.fromLong(r.stats.pushes),
      "pullRequests" -> Json.fromLong(r.stats.pullRequests),
      "merged" -> Json.fromLong(r.stats.mergedPullRequests),
      "issues" -> Json.fromLong(r.stats.issues),
      "comments" -> Json.fromLong(r.stats.comments),
      "stars" -> Json.fromLong(r.stats.stars),
      "forks" -> Json.fromLong(r.stats.forks)
    )
  }

  private def burstJson(b: LiveBurst): Json =
    Json.obj(
      "actor" -> Json.fromString(b.actor),
      "isBot" -> Json.fromBoolean(b.isBot),
      "events" -> Json.fromInt(b.events),
      "startedAt" -> Json.fromLong(b.startedAt),
      "endedAt" -> Json.fromLong(b.endedAt),
      "repos" -> Json.fromValues(b.repos.map { h =>
        Json.obj("repo" -> Json.fromString(h.repo), "events" -> Json.fromInt(h.events))
      })
    )
}

object Dashboard {
  val empty: Dashboard = Dashboard(Vector.empty, 0L, 0L, Vector.empty, 0L, Map.empty)
}

/** Folds bursts and leaderboard rows into one [[Dashboard]] and writes it to disk.
  *
  * Runs with parallelism one behind a constant key, which is fine: its input is a few dozen records
  * per minute. The state lives in plain fields rather than Flink state on purpose — after a restart
  * the job replays the poller's retained files and rebuilds it within a window.
  *
  * Writes are debounced on a processing-time timer, so one leaderboard of fifteen rows produces one
  * write rather than fifteen, and each write is a rename over the previous file, so the web server
  * never serves a half-written snapshot.
  */
class DashboardWriter(
    stateFile: String,
    settings: DashboardSettings,
    maxBursts: Int,
    flushMillis: Long
) extends KeyedCoProcessFunction[Int, LiveBurst, RankedRepo, String] {

  @transient private var dashboard: Dashboard = _
  @transient private var flushScheduled: Boolean = _
  @transient private var dirty: Boolean = _

  override def open(parameters: Configuration): Unit = {
    dashboard = Dashboard.empty
    flushScheduled = false
    dirty = false
  }

  override def processElement1(
      burst: LiveBurst,
      ctx: KeyedCoProcessFunction[Int, LiveBurst, RankedRepo, String]#Context,
      out: Collector[String]
  ): Unit = update(dashboard.withBurst(burst, maxBursts), ctx)

  override def processElement2(
      ranked: RankedRepo,
      ctx: KeyedCoProcessFunction[Int, LiveBurst, RankedRepo, String]#Context,
      out: Collector[String]
  ): Unit = update(dashboard.withRanked(ranked), ctx)

  private def update(
      next: Dashboard,
      ctx: KeyedCoProcessFunction[Int, LiveBurst, RankedRepo, String]#Context
  ): Unit = {
    dashboard = next
    dirty = true
    if (!flushScheduled) {
      val now = ctx.timerService().currentProcessingTime()
      ctx.timerService().registerProcessingTimeTimer(now + flushMillis)
      flushScheduled = true
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedCoProcessFunction[Int, LiveBurst, RankedRepo, String]#OnTimerContext,
      out: Collector[String]
  ): Unit = {
    flushScheduled = false
    flush()
  }

  /** A bounded run (`--once`, and the tests) ends before the last timer fires; write on the way out
    * so it still leaves a complete snapshot behind.
    */
  override def close(): Unit = if (dashboard != null) flush()

  private def flush(): Unit = if (dirty) {
    DashboardWriter.writeAtomically(
      Paths.get(stateFile),
      dashboard.toJson(settings, System.currentTimeMillis()).noSpaces
    )
    dirty = false
  }
}

object DashboardWriter {

  /** Writes beside the target under a hidden name, then renames over it. */
  def writeAtomically(target: Path, content: String): Unit = {
    Option(target.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    val tmp = target.resolveSibling(s".${target.getFileName}.tmp")
    Files.write(tmp, content.getBytes(StandardCharsets.UTF_8))
    Files.move(
      tmp,
      target,
      StandardCopyOption.ATOMIC_MOVE,
      StandardCopyOption.REPLACE_EXISTING
    )
  }
}
