package ghpulse.spark

import ghpulse.core.analytics.{ScoreWeights, TimeBucket}
import ghpulse.core.testkit.Fixtures
import ghpulse.core.testkit.Fixtures.{Hour, T0}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers

class ActivityTimelineSpec extends SparkSuite with Matchers {

  private lazy val timeline: DataFrame =
    ActivityTimeline.compute(scenarioEvents(), Hour, ScoreWeights.default).cache()

  private def row(repo: String, windowStart: Long): Row =
    timeline.filter(col("repo") === repo && col("windowStart") === windowStart).head()

  private def totalScore(df: DataFrame): Double = df.agg(sum("score")).head().getDouble(0)

  test("buckets events into windows aligned with the core time grid") {
    val starts = timeline.select("windowStart").distinct().collect().map(_.getLong(0)).sorted
    starts shouldBe Array(T0, T0 + Hour)
    starts.foreach(start => start shouldBe TimeBucket.floor(start, Hour))
  }

  test("one row per repository per window it was active in") {
    timeline.count() shouldBe 5L // rocket and probe in both hours, telemetry in the first
  }

  test("scores each window from the events that fall inside it") {
    row("acme/rocket", T0).getAs[Double]("score") shouldBe 25.0
    row("acme/rocket", T0 + Hour).getAs[Double]("score") shouldBe 5.0
    row("acme/telemetry", T0).getAs[Double]("score") shouldBe 12.0
    row("acme/probe", T0).getAs[Double]("score") shouldBe 5.0
  }

  test("the rolling frame accumulates a repository's own history") {
    row("acme/rocket", T0).getAs[Double]("rolling_score") shouldBe 25.0
    row("acme/rocket", T0 + Hour).getAs[Double]("rolling_score") shouldBe 30.0
  }

  test("period-over-period change is null-safe on a repository's first window") {
    val first = row("acme/rocket", T0)
    first.isNullAt(first.fieldIndex("previous_score")) shouldBe true
    first.getAs[Double]("score_delta") shouldBe 25.0

    val second = row("acme/rocket", T0 + Hour)
    second.getAs[Double]("previous_score") shouldBe 25.0
    second.getAs[Double]("score_delta") shouldBe -20.0
  }

  test("ranking is computed within a window, not across the whole timeline") {
    val firstHour = timeline
      .filter(col("windowStart") === T0)
      .collect()
      .map(r => r.getAs[String]("repo") -> r.getAs[Int]("rank_in_window"))

    firstHour should contain theSameElementsAs
      Array("acme/rocket" -> 1, "acme/telemetry" -> 2, "acme/probe" -> 3)

    // The second window starts counting again from 1 rather than continuing at 4.
    row("acme/rocket", T0 + Hour).getAs[Int]("rank_in_window") shouldBe 1
  }

  test("window labels agree with the core formatter") {
    row("acme/rocket", T0).getAs[String]("window_label") shouldBe TimeBucket.formatMinute(T0)
    row("acme/rocket", T0).getAs[String]("window_label") shouldBe "2024-01-15T15:00Z"
  }

  test("a narrower window splits the same events into more rows without losing any") {
    val quarterly = ActivityTimeline.compute(scenarioEvents(), Hour / 4, ScoreWeights.default)

    quarterly.count() should be > timeline.count()
    totalScore(quarterly) shouldBe totalScore(timeline)
  }

  test("leadersPerWindow keeps only the top ranks of each window") {
    val leaders = ActivityTimeline.leadersPerWindow(timeline, perWindow = 2).collect()

    leaders.map(_.getAs[Int]("rank_in_window")).distinct.sorted shouldBe Array(1, 2)
    leaders should have size 4 // two windows, two leaders each
  }

  test("every scenario event is accounted for exactly once") {
    timeline.agg(sum("stats.events")).head().getLong(0) shouldBe Fixtures.scenario.size.toLong
  }
}
