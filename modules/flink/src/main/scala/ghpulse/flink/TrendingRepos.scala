package ghpulse.flink

import ghpulse.core.analytics.{ActivityStats, RepoActivity, ScoreWeights, TopK, WindowedActivity}
import ghpulse.core.domain.GitHubEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/** One entry of a window's leaderboard, flat so it sinks cleanly to any format. */
final case class RankedRepo(
    windowStart: Long,
    windowEnd: Long,
    rank: Int,
    repo: String,
    score: Double,
    stats: ActivityStats
)

/** The repositories trending in each window.
  *
  * Ranking a stream is a two-stage problem, and doing it in one stage is the classic mistake:
  * `windowAll` would funnel every event through a single parallel instance. Instead the first stage
  * aggregates per repository — parallel across the whole cluster — and only the small
  * per-repository results are re-keyed by window end for the second stage to rank. The amount of
  * data crossing the second shuffle is bounded by the number of active repositories, not by the
  * event rate.
  */
object TrendingRepos {

  def apply(
      events: DataStream[GitHubEvent],
      sizeMillis: Long,
      slideMillis: Long,
      k: Int,
      weights: ScoreWeights
  ): DataStream[RankedRepo] =
    rank(WindowedActivityStream.sliding(events, sizeMillis, slideMillis, weights), k)

  /** Second stage: collect a window's per-repository results and emit its top `k`. */
  def rank(activity: DataStream[WindowedActivity], k: Int): DataStream[RankedRepo] =
    activity
      .keyBy(_.windowEnd)
      .process(new RankWindow(k))
      .name(s"top-$k")
}

/** Buffers one window's results and ranks them when event time says the window is complete.
  *
  * Keyed by window end, so the timer that fires at that exact timestamp is the signal that no
  * further results for the window can arrive — the streaming equivalent of the batch job's "all
  * rows are in, now sort".
  */
class RankWindow(k: Int) extends KeyedProcessFunction[Long, WindowedActivity, RankedRepo] {

  @transient private var candidates: ListState[WindowedActivity] = _

  override def open(parameters: Configuration): Unit =
    candidates = getRuntimeContext.getListState(
      new ListStateDescriptor("candidates", createTypeInformation[WindowedActivity])
    )

  override def processElement(
      value: WindowedActivity,
      ctx: KeyedProcessFunction[Long, WindowedActivity, RankedRepo]#Context,
      out: Collector[RankedRepo]
  ): Unit = {
    candidates.add(value)
    // The key *is* the window end, so every element registers the same timer and Flink keeps one.
    ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey)
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, WindowedActivity, RankedRepo]#OnTimerContext,
      out: Collector[RankedRepo]
  ): Unit = {
    val buffered = candidates.get().asScala.toList
    candidates.clear()

    if (buffered.nonEmpty) {
      val windowStart = buffered.head.windowStart
      val top: List[RepoActivity] = TopK.from(k, buffered.map(_.toRepoActivity)).toList

      top.zipWithIndex.foreach { case (activity, index) =>
        out.collect(
          RankedRepo(
            windowStart,
            timestamp,
            index + 1,
            activity.repo,
            activity.score,
            activity.stats
          )
        )
      }
    }
  }
}
