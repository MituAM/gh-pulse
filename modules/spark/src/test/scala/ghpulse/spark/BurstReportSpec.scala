package ghpulse.spark

import ghpulse.core.analytics.{ActorBurst, BurstConfig}
import ghpulse.core.testkit.Fixtures.{Minute, T0}
import ghpulse.core.testkit.{Fixtures, Reference}
import org.scalatest.matchers.should.Matchers

class BurstReportSpec extends SparkSuite with Matchers {

  private val config = BurstConfig.default

  test("finds the actor whose events are dense enough to qualify") {
    val bursts = BurstReport.compute(scenarioEvents(), config).collect()

    bursts should have size 1
    bursts.head.actor shouldBe "bot"
    bursts.head.events shouldBe config.minEvents
    bursts.head.startedAt shouldBe T0 + 40 * Minute
    bursts.head.endedAt shouldBe T0 + 40 * Minute + 9 * 20000L
  }

  test("actors below the threshold are not reported") {
    val reported = BurstReport.compute(scenarioEvents(), config).collect().map(_.actor).toSet
    val busiest = Fixtures.scenario.groupBy(_.actor.login).mapValues(_.size)

    busiest.filter(_._2 < config.minEvents).keys.foreach(actor => reported should not contain actor)
  }

  test("the distributed result matches the single-threaded reference") {
    val actual = BurstReport.compute(scenarioEvents(), config).collect().toList
    actual.sorted(ActorBurst.ordering) shouldBe Reference.bursts(config)
  }

  test("shuffling the input across partitions does not change the answer") {
    val answers = Seq(1, 2, 7).map { partitions =>
      BurstReport.compute(scenarioEvents(partitions), config).collect().sortBy(_.endedAt).toList
    }
    answers.distinct should have size 1
  }

  test("a lower threshold reports more actors, and still matches the reference") {
    val sensitive = config.copy(minEvents = 3, windowMillis = 30 * Minute)
    val actual = BurstReport.compute(scenarioEvents(), sensitive).collect().toList

    actual.sorted(ActorBurst.ordering) shouldBe Reference.bursts(sensitive)
    actual.map(_.actor).toSet should contain("bot")
    // Three of ada's four events fall inside the same half hour.
    actual.map(_.actor).toSet should contain("ada")
  }

  test("renders a table with one line per burst plus a header") {
    val bursts = BurstReport.compute(scenarioEvents(), config).collect().toSeq
    val lines = BurstReport.render(bursts).split("\n")

    lines should have size 2
    lines(1) should include("bot")
    lines(1) should include("2024-01-15T15:40Z")
  }
}
