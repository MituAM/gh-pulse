package ghpulse.core.analytics

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/** Aligns event timestamps to fixed windows.
  *
  * Batch and streaming must agree on where a window starts or their results cannot be compared, so
  * both engines bucket through this one function instead of each reaching for their own date
  * helpers (Spark SQL's `date_trunc`, Flink's window assigners).
  */
object TimeBucket {

  val Hour: Long = 3600000L
  val Day: Long = 24 * Hour

  /** Floors `timestamp` to the start of the window of width `size` containing it. */
  def floor(timestamp: Long, size: Long): Long = {
    require(size > 0, s"bucket size must be positive, got $size")
    Math.floorDiv(timestamp, size) * size
  }

  private val hourFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'").withZone(ZoneOffset.UTC)
  private val dayFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)

  /** UTC label for a bucket start, e.g. `2024-01-15T15:00Z`. */
  def formatMinute(timestamp: Long): String = hourFormat.format(Instant.ofEpochMilli(timestamp))

  /** UTC calendar day of a timestamp, e.g. `2024-01-15`. */
  def formatDay(timestamp: Long): String = dayFormat.format(Instant.ofEpochMilli(timestamp))
}
