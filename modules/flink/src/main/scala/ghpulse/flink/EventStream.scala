package ghpulse.flink

import ghpulse.core.codec.EventDecoder
import ghpulse.core.domain.GitHubEvent
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.time.Duration

object EventStream {

  /** Replays newline-delimited JSON as an event-time stream.
    *
    * Reading a file is a stand-in for a real source — swapping in Kafka means replacing this one
    * `fromSource` call, because everything downstream is expressed against
    * `DataStream[GitHubEvent]` and the watermarks assigned here, not against where the bytes came
    * from.
    */
  def fromPath(
      env: StreamExecutionEnvironment,
      path: String,
      decoder: EventDecoder,
      maxOutOfOrderness: Duration
  ): DataStream[GitHubEvent] = {
    val source = FileSource
      .forRecordStreamFormat(new TextLineInputFormat(), new Path(path))
      .build()

    // Kept short deliberately: Flink builds operator metric names out of it and warns past 80
    // characters, which a full input path passes easily.
    val lines = env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "events")
    withEventTime(lines.flatMap(new DecodeEvent(decoder)), maxOutOfOrderness)
  }

  /** Assigns event time and the watermarks that decide when a window may close.
    *
    * `maxOutOfOrderness` is the one real tuning knob in the streaming job: GH Archive genuinely
    * delivers events a little out of order, and a watermark that runs ahead of them silently drops
    * the stragglers. Batch has no equivalent because it sorts everything before it starts, which is
    * the entire difference in cost between the two jobs.
    */
  def withEventTime(
      events: DataStream[GitHubEvent],
      maxOutOfOrderness: Duration
  ): DataStream[GitHubEvent] =
    events.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[GitHubEvent](maxOutOfOrderness)
        .withTimestampAssigner(new SerializableTimestampAssigner[GitHubEvent] {
          override def extractTimestamp(event: GitHubEvent, recordTimestamp: Long): Long =
            event.createdAt
        })
        // Without this a partition that goes quiet holds the whole job's watermark back, and no
        // window ever closes.
        .withIdleness(Duration.ofSeconds(10))
    )
}

/** Decodes records and counts the ones it cannot.
  *
  * Rich rather than a plain lambda so the rejects land on a Flink metric, which is what makes a
  * creeping schema change visible in a dashboard instead of as quietly missing rows.
  */
class DecodeEvent(decoder: EventDecoder) extends RichFlatMapFunction[String, GitHubEvent] {

  private val guarded = EventDecoder.guarded(decoder)
  @transient private var rejected: Counter = _

  override def open(parameters: Configuration): Unit =
    rejected = getRuntimeContext.getMetricGroup.counter("rejectedRecords")

  override def flatMap(line: String, out: Collector[GitHubEvent]): Unit =
    guarded.decode(line) match {
      case Right(event) => out.collect(event)
      case Left(_) => rejected.inc()
    }
}
