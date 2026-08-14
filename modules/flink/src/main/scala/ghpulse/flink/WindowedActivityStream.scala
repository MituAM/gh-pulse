package ghpulse.flink

import ghpulse.core.analytics.{ActivityStats, ScoreWeights, WindowedActivity}
import ghpulse.core.domain.GitHubEvent
import ghpulse.core.typeclass.Monoid
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{
  SlidingEventTimeWindows,
  TumblingEventTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/** Per-repository activity over event-time windows.
  *
  * The aggregation is incremental: [[StatsAggregator]] folds each event into a running
  * `ActivityStats` as it arrives, so a window holds one small record rather than every event that
  * fell into it. That is only possible because the combine is a monoid — the same property the
  * Spark job relies on to re-associate across partitions.
  */
object WindowedActivityStream {

  /** Non-overlapping windows: each event contributes to exactly one. */
  def tumbling(
      events: DataStream[GitHubEvent],
      windowMillis: Long,
      weights: ScoreWeights
  ): DataStream[WindowedActivity] =
    events
      .keyBy(_.repo.name)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowMillis)))
      .aggregate(new StatsAggregator, new AttachWindow(weights))
      .name(s"activity/${windowMillis}ms")

  /** Overlapping windows: a "last hour, refreshed every ten minutes" view.
    *
    * Each event now lands in `size / slide` windows at once, which is why the incremental
    * aggregation matters even more here than above.
    */
  def sliding(
      events: DataStream[GitHubEvent],
      sizeMillis: Long,
      slideMillis: Long,
      weights: ScoreWeights
  ): DataStream[WindowedActivity] =
    events
      .keyBy(_.repo.name)
      .window(
        SlidingEventTimeWindows.of(Time.milliseconds(sizeMillis), Time.milliseconds(slideMillis))
      )
      .aggregate(new StatsAggregator, new AttachWindow(weights))
      .name(s"activity/${sizeMillis}ms every ${slideMillis}ms")
}

/** The shared monoid, wearing Flink's interface.
  *
  * `merge` is required for session windows and for merging pre-aggregates, and it is precisely
  * `combine` — the abstraction lines up with the runtime's demands exactly.
  */
class StatsAggregator extends AggregateFunction[GitHubEvent, ActivityStats, ActivityStats] {
  private val monoid = Monoid[ActivityStats]

  override def createAccumulator(): ActivityStats = monoid.empty
  override def add(event: GitHubEvent, acc: ActivityStats): ActivityStats =
    monoid.combine(acc, ActivityStats.of(event))
  override def getResult(acc: ActivityStats): ActivityStats = acc
  override def merge(a: ActivityStats, b: ActivityStats): ActivityStats = monoid.combine(a, b)
}

/** Attaches the window boundaries to the aggregate.
  *
  * Runs once per window over a single element — the pre-aggregated result — so pairing a
  * `ProcessWindowFunction` with an `AggregateFunction` buys the window metadata without giving up
  * incremental aggregation.
  */
class AttachWindow(weights: ScoreWeights)
    extends ProcessWindowFunction[ActivityStats, WindowedActivity, String, TimeWindow] {

  override def process(
      repo: String,
      context: Context,
      elements: Iterable[ActivityStats],
      out: Collector[WindowedActivity]
  ): Unit = {
    val stats = elements.head
    out.collect(
      WindowedActivity(
        windowStart = context.window.getStart,
        windowEnd = context.window.getEnd,
        repo = repo,
        stats = stats,
        score = weights.applyTo(stats)
      )
    )
  }
}
