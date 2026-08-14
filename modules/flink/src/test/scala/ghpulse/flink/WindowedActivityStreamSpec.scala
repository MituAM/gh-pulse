package ghpulse.flink

import ghpulse.core.analytics.ScoreWeights
import ghpulse.core.testkit.Fixtures.{Hour, T0}
import ghpulse.core.testkit.{Fixtures, Reference}
import org.scalatest.matchers.should.Matchers

class WindowedActivityStreamSpec extends FlinkSuite with Matchers {

  private val weights = ScoreWeights.default

  test("tumbling windows reproduce the reference aggregation exactly") {
    val env = environment()
    val windows = collect(WindowedActivityStream.tumbling(scenarioStream(env), Hour, weights))

    windows should contain theSameElementsAs Reference.windows(Hour, weights)
  }

  test("window boundaries are aligned to the epoch, not to the first event") {
    val env = environment()
    val windows = collect(WindowedActivityStream.tumbling(scenarioStream(env), Hour, weights))

    windows.map(_.windowStart).distinct.sorted shouldBe List(T0, T0 + Hour)
    windows.foreach(w => (w.windowEnd - w.windowStart) shouldBe Hour)
  }

  test("scores the scenario's first hour as the batch job does") {
    val env = environment()
    val windows = collect(WindowedActivityStream.tumbling(scenarioStream(env), Hour, weights))
    val firstHour = windows.filter(_.windowStart == T0).map(w => w.repo -> w.score).toMap

    firstHour shouldBe Map(
      "acme/rocket" -> 25.0,
      "acme/telemetry" -> 12.0,
      "acme/probe" -> 5.0
    )
  }

  test("every event lands in exactly one tumbling window") {
    val env = environment()
    val windows = collect(WindowedActivityStream.tumbling(scenarioStream(env), Hour, weights))

    windows.map(_.stats.events).sum shouldBe Fixtures.scenario.size.toLong
  }

  test("an out-of-order event is still placed in the window it belongs to") {
    val env = environment()
    val windows = collect(WindowedActivityStream.tumbling(scenarioStream(env), Hour, weights))
    val probeFirstHour = windows.find(w => w.repo == "acme/probe" && w.windowStart == T0)

    // p4 arrives after p3 but happened before it; both belong to the first hour, and the two
    // commits it carries must be counted there.
    probeFirstHour.map(_.stats.commits) shouldBe Some(3L)
  }

  test("sliding windows overlap, so an event is counted once per window it falls in") {
    val env = environment()
    val sliding = collect(
      WindowedActivityStream.sliding(scenarioStream(env), Hour, Hour / 2, weights)
    )

    sliding.map(_.stats.events).sum shouldBe (2 * Fixtures.scenario.size).toLong
    sliding.size should be > Reference.windows(Hour, weights).size
  }

  test("a narrower window splits the same events without losing any") {
    val env = environment()
    val quarterly = collect(WindowedActivityStream.tumbling(scenarioStream(env), Hour / 4, weights))

    quarterly should contain theSameElementsAs Reference.windows(Hour / 4, weights)
    quarterly.map(_.stats.events).sum shouldBe Fixtures.scenario.size.toLong
  }
}
