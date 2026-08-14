package ghpulse.flink

import ghpulse.core.analytics.{ActorBurst, BurstConfig, BurstDetector, BurstState}
import ghpulse.core.domain.GitHubEvent
import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/** The streaming half of burst detection.
  *
  * The rule is [[BurstDetector]] unchanged — shared with the Spark job — so all this operator
  * contributes is *when* the rule gets to see each event.
  */
object PushBursts {

  def apply(events: DataStream[GitHubEvent], config: BurstConfig): DataStream[ActorBurst] =
    events
      .keyBy(_.actor.login)
      .process(new DetectBursts(config))
      .name("bursts")
}

/** Feeds one actor's events to the detector in event-time order.
  *
  * `processElement` sees records in *arrival* order, which for an out-of-order stream is not the
  * order they happened in. Since the detector's answer depends on that order, events are buffered
  * against an event-time timer and only replayed once the watermark has passed them: at that point
  * no earlier event can still arrive, so the sequence handed to the rule is the same sequence the
  * batch job sorts into. That is what makes `BatchStreamParitySpec` pass rather than nearly pass.
  *
  * At the end of a bounded stream Flink emits a final watermark at `Long.MaxValue`, which drains
  * whatever is still buffered.
  */
class DetectBursts(config: BurstConfig)
    extends KeyedProcessFunction[String, GitHubEvent, ActorBurst] {

  @transient private var pending: ListState[Long] = _
  @transient private var detector: ValueState[BurstState] = _

  override def open(parameters: Configuration): Unit = {
    pending = getRuntimeContext.getListState(
      new ListStateDescriptor("pending", createTypeInformation[Long])
    )
    detector = getRuntimeContext.getState(
      new ValueStateDescriptor("detector", createTypeInformation[BurstState])
    )
  }

  override def processElement(
      event: GitHubEvent,
      ctx: KeyedProcessFunction[String, GitHubEvent, ActorBurst]#Context,
      out: Collector[ActorBurst]
  ): Unit = {
    pending.add(event.createdAt)
    ctx.timerService().registerEventTimeTimer(event.createdAt)
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, GitHubEvent, ActorBurst]#OnTimerContext,
      out: Collector[ActorBurst]
  ): Unit = {
    val buffered = pending.get().asScala.toVector
    val (due, later) = buffered.partition(_ <= timestamp)

    val finalState = due.sorted.foldLeft(Option(detector.value()).getOrElse(BurstState.empty)) {
      (state, at) =>
        val (next, burst) = BurstDetector.step(state, at, config)
        burst.foreach(b => out.collect(ActorBurst(ctx.getCurrentKey, b)))
        next
    }

    detector.update(finalState)
    pending.update(later.asJava)
  }
}
