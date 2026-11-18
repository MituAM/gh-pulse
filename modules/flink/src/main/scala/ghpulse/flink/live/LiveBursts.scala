package ghpulse.flink.live

import ghpulse.core.analytics.{Burst, BurstConfig, BurstDetector, BurstState}
import ghpulse.core.domain.GitHubEvent
import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  StateTtlConfig,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/** One event by an actor, reduced to what burst detection needs: when, and which repository. */
final case class Hit(at: Long, repo: String)

/** How many of a burst's events landed on one repository. */
final case class RepoHits(repo: String, events: Int)

/** A burst as the dashboard shows it: who, how many events, when, and which repositories. */
final case class LiveBurst(
    actor: String,
    events: Int,
    startedAt: Long,
    endedAt: Long,
    repos: List[RepoHits]
) {
  def isBot: Boolean = LiveBurst.isBot(actor)
}

object LiveBurst {

  /** GitHub App accounts are the only logins allowed to end in `[bot]`. */
  def isBot(login: String): Boolean = login.endsWith("[bot]")
}

object LiveBursts {

  /** How many target repositories a burst lists. */
  val ReposPerBurst: Int = 5

  /** Forget an actor who has been quiet this long. A live job sees millions of distinct actors;
    * without this its state would only ever grow.
    */
  val StateTtlMillis: Long = 60 * 60 * 1000L

  def apply(events: DataStream[GitHubEvent], config: BurstConfig): DataStream[LiveBurst] =
    events
      .keyBy(_.actor.login)
      .process(new DetectLiveBursts(config))
      .name("live-bursts")

  /** The repositories a burst hit, busiest first. */
  def targets(hits: Seq[Hit], burst: Burst): List[RepoHits] =
    hits
      .filter(h => h.at >= burst.startedAt && h.at <= burst.endedAt)
      .groupBy(_.repo)
      .map { case (repo, inRepo) => RepoHits(repo, inRepo.size) }
      .toList
      .sortBy(r => (-r.events, r.repo))
      .take(ReposPerBurst)
}

/** Burst detection for the live job: [[ghpulse.flink.DetectBursts]] plus the repositories hit.
  *
  * Same approach as the parity-tested operator: buffer each event until the watermark passes it,
  * then feed `BurstDetector.step` in event-time order. On top of that it keeps the recent hits —
  * only those inside the detector's window — to say which repositories a burst targeted.
  */
class DetectLiveBursts(config: BurstConfig)
    extends KeyedProcessFunction[String, GitHubEvent, LiveBurst] {

  /** Events that arrived but whose time the watermark has not reached yet. */
  @transient private var pending: ListState[Hit] = _

  /** Events already given to the detector that are still inside its window. */
  @transient private var recent: ListState[Hit] = _

  @transient private var detector: ValueState[BurstState] = _

  override def open(parameters: Configuration): Unit = {
    val ttl = StateTtlConfig
      .newBuilder(Time.milliseconds(LiveBursts.StateTtlMillis))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .cleanupIncrementally(10, false)
      .build()

    val pendingDesc = new ListStateDescriptor("pending", createTypeInformation[Hit])
    val recentDesc = new ListStateDescriptor("recent", createTypeInformation[Hit])
    val detectorDesc = new ValueStateDescriptor("detector", createTypeInformation[BurstState])
    Seq(pendingDesc, recentDesc, detectorDesc).foreach(_.enableTimeToLive(ttl))

    pending = getRuntimeContext.getListState(pendingDesc)
    recent = getRuntimeContext.getListState(recentDesc)
    detector = getRuntimeContext.getState(detectorDesc)
  }

  override def processElement(
      event: GitHubEvent,
      ctx: KeyedProcessFunction[String, GitHubEvent, LiveBurst]#Context,
      out: Collector[LiveBurst]
  ): Unit = {
    pending.add(Hit(event.createdAt, event.repo.name))
    ctx.timerService().registerEventTimeTimer(event.createdAt)
  }

  /** The watermark reached `timestamp`: every pending event up to it can now be processed. */
  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, GitHubEvent, LiveBurst]#OnTimerContext,
      out: Collector[LiveBurst]
  ): Unit = {
    val (due, later) = pending.get().asScala.toVector.partition(_.at <= timestamp)
    pending.update(later.asJava)
    if (due.isEmpty) return

    var state = Option(detector.value()).getOrElse(BurstState.empty)
    var hits = recent.get().asScala.toVector

    for (hit <- due.sortBy(_.at)) {
      hits :+= hit
      val (next, burst) = BurstDetector.step(state, hit.at, config)
      state = next
      for (b <- burst) {
        val repos = LiveBursts.targets(hits, b)
        out.collect(LiveBurst(ctx.getCurrentKey, b.events, b.startedAt, b.endedAt, repos))
      }
    }

    // Keep exactly the hits the detector still has in its window.
    val windowStart = state.recent.headOption.getOrElse(Long.MaxValue)
    detector.update(state)
    recent.update(hits.filter(_.at >= windowStart).asJava)
  }
}
