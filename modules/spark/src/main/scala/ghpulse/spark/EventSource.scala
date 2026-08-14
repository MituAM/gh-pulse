package ghpulse.spark

import ghpulse.core.codec.EventDecoder
import ghpulse.core.domain.GitHubEvent
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.LongAccumulator

/** How many records were read and how many could not be understood. */
final case class IngestReport(accepted: Long, rejected: Long) {
  def total: Long = accepted + rejected
  def rejectionRate: Double = if (total == 0) 0.0 else rejected.toDouble / total
}

object EventSource {

  /** Reads newline-delimited JSON into typed events.
    *
    * Text is read line by line and handed to the shared [[EventDecoder]] rather than to Spark's own
    * JSON reader. Spark's reader would be faster and splittable, but it would also be a *second*
    * parser: the Flink job has to parse record-at-a-time, and two independently written parsers
    * drift. Sharing one decoder is what makes `BatchStreamParitySpec` a meaningful test.
    *
    * Spark accepts a file, a directory or a glob here, and decompresses `.gz` transparently.
    */
  def read(
      spark: SparkSession,
      path: String,
      decoder: EventDecoder
  ): (Dataset[GitHubEvent], LongAccumulator) = {
    import spark.implicits._

    val rejected = spark.sparkContext.longAccumulator("rejectedRecords")
    // Bound to a local so the closure ships the decoder itself, not the enclosing object.
    val decode = EventDecoder.guarded(decoder)

    val events = spark.read
      .textFile(path)
      .flatMap { line =>
        decode.decode(line) match {
          case Right(event) => Some(event)
          case Left(_) =>
            // A public firehose always contains some records this schema cannot read. Counting
            // them beats failing the job, and beats discarding them silently.
            rejected.add(1L)
            None
        }
      }

    (events, rejected)
  }

  /** Forces the read and reports what it saw.
    *
    * The accumulator is only populated once the events have actually been computed, so a caller
    * that wants the report has to trigger an action; doing it here keeps that subtlety in one
    * place. The dataset is cached because every job downstream traverses it again.
    */
  def readAndReport(
      spark: SparkSession,
      path: String,
      decoder: EventDecoder
  ): (Dataset[GitHubEvent], IngestReport) = {
    val (events, rejected) = read(spark, path, decoder)
    events.cache()
    val accepted = events.count()
    (events, IngestReport(accepted, rejected.value))
  }
}
