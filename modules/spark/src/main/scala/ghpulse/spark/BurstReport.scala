package ghpulse.spark

import ghpulse.core.analytics.{ActorBurst, BurstConfig, BurstDetector, TimeBucket}
import ghpulse.core.domain.GitHubEvent
import org.apache.spark.sql.Dataset

/** The batch half of burst detection.
  *
  * The rule itself is [[BurstDetector]], shared verbatim with the Flink job. All this adds is the
  * distribution: shuffle each actor's timestamps to one place, put them in event-time order, and
  * replay them. Because the streaming job feeds the same function the same sequence, the two
  * produce the same bursts — which is what `BatchStreamParitySpec` checks.
  */
object BurstReport {

  def compute(events: Dataset[GitHubEvent], config: BurstConfig): Dataset[ActorBurst] = {
    val spark = events.sparkSession
    import spark.implicits._

    val burstConfig = config

    events
      .map(event => (event.actor.login, event.createdAt))
      .groupByKey(_._1)
      .flatMapGroups { (actor, rows) =>
        // Safe to materialise: this is one actor's timestamps within the analysed period, which is
        // orders of magnitude smaller than the partition it arrived in.
        val timestamps = rows.map(_._2).toVector.sorted
        BurstDetector.detect(timestamps, burstConfig).map(ActorBurst(actor, _))
      }
  }

  def render(bursts: Seq[ActorBurst]): String = {
    val header = f"${"actor"}%-25s ${"events"}%7s  ${"from"}%-20s ${"to"}%-20s"
    val rows = bursts.map { b =>
      f"${b.actor.take(25)}%-25s ${b.events}%7d  " +
        f"${TimeBucket.formatMinute(b.startedAt)}%-20s ${TimeBucket.formatMinute(b.endedAt)}%-20s"
    }
    (header +: rows).mkString("\n")
  }
}
