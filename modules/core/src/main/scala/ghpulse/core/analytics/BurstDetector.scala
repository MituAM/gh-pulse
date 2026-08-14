package ghpulse.core.analytics

/** A run of at least [[BurstConfig.minEvents]] events inside one [[BurstConfig.window]]. */
final case class Burst(startedAt: Long, endedAt: Long, events: Int)

/** @param minEvents
  *   how many events inside the window constitute a burst.
  * @param windowMillis
  *   the width of the sliding window, in milliseconds.
  * @param cooldownMillis
  *   how long to stay quiet after reporting, so one sustained burst reports once rather than on
  *   every subsequent event.
  */
final case class BurstConfig(minEvents: Int, windowMillis: Long, cooldownMillis: Long) {
  require(minEvents > 1, s"minEvents must be greater than 1, got $minEvents")
  require(windowMillis > 0, s"windowMillis must be positive, got $windowMillis")
}

object BurstConfig {

  /** Ten events from one actor within five minutes, reporting at most once per fifteen. */
  val default: BurstConfig = BurstConfig(
    minEvents = 10,
    windowMillis = 5 * 60 * 1000L,
    cooldownMillis = 15 * 60 * 1000L
  )
}

/** The detector's memory between events: recent timestamps and when it last reported. */
final case class BurstState(recent: Vector[Long], lastReportAt: Option[Long])

object BurstState {
  val empty: BurstState = BurstState(Vector.empty, None)
}

/** Detects unusually dense activity from a single actor.
  *
  * The whole rule is one pure `step` function over an explicit state value, which is what allows
  * the identical rule to run in two very different places: Flink keeps [[BurstState]] in keyed
  * `ValueState` and calls `step` per record, while the Spark job folds `step` over each actor's
  * sorted history. `BatchStreamParitySpec` pins the two together.
  *
  * `step` tolerates out-of-order input — it re-sorts its small window rather than assuming
  * monotonic timestamps — because a real stream arrives out of order and a batch job that sorted
  * perfectly would otherwise disagree with it.
  */
object BurstDetector extends Serializable {

  /** Folds one timestamp into the state, reporting a burst if this event completed one.
    *
    * @return
    *   the next state, and a [[Burst]] if this event tipped the window over the threshold.
    */
  def step(state: BurstState, timestamp: Long, config: BurstConfig): (BurstState, Option[Burst]) = {
    val ordered = (state.recent :+ timestamp).sorted
    val latest = ordered.last
    val window = ordered.dropWhile(_ <= latest - config.windowMillis)

    val cooledDown = state.lastReportAt.forall(latest - _ >= config.cooldownMillis)
    val isBurst = window.size >= config.minEvents && cooledDown

    val next = BurstState(window, if (isBurst) Some(latest) else state.lastReportAt)
    val burst = if (isBurst) Some(Burst(window.head, latest, window.size)) else None
    (next, burst)
  }

  /** Replays a whole history through [[step]]. The batch entry point. */
  def detect(timestamps: TraversableOnce[Long], config: BurstConfig): List[Burst] = {
    val (_, bursts) = timestamps.foldLeft((BurstState.empty, List.empty[Burst])) {
      case ((state, acc), ts) =>
        val (next, burst) = step(state, ts, config)
        (next, burst.fold(acc)(_ :: acc))
    }
    bursts.reverse
  }
}
