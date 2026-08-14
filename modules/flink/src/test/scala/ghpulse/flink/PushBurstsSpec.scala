package ghpulse.flink

import ghpulse.core.analytics.{ActorBurst, BurstConfig}
import ghpulse.core.testkit.Fixtures.{Minute, T0}
import ghpulse.core.testkit.{Fixtures, Reference}
import org.scalatest.matchers.should.Matchers

class PushBurstsSpec extends FlinkSuite with Matchers {

  private val config = BurstConfig.default

  private def detect(cfg: BurstConfig = config): List[ActorBurst] = {
    val env = environment()
    collect(PushBursts(scenarioStream(env), cfg)).sorted(ActorBurst.ordering)
  }

  test("finds the actor whose events are dense enough to qualify") {
    val bursts = detect()

    bursts should have size 1
    bursts.head.actor shouldBe "bot"
    bursts.head.events shouldBe config.minEvents
    bursts.head.startedAt shouldBe T0 + 40 * Minute
  }

  test("reproduces the reference detection exactly") {
    detect() shouldBe Reference.bursts(config)
  }

  test("a lower threshold reports the same actors the reference does") {
    val sensitive = config.copy(minEvents = 3, windowMillis = 30 * Minute)
    detect(sensitive) shouldBe Reference.bursts(sensitive)
  }

  test("events replayed out of order still produce the chronological answer") {
    // The fixture's arrival order is not its event-time order, so this only holds because the
    // operator buffers against an event-time timer instead of trusting arrival order.
    val arrivalOrder = Fixtures.scenario.map(_.createdAt)
    arrivalOrder should not be arrivalOrder.sorted

    detect() shouldBe Reference.bursts(config)
  }

  test("a threshold nobody reaches produces nothing") {
    detect(config.copy(minEvents = 50)) shouldBe empty
  }

  test("keyed state isolates actors from one another") {
    // Every one of bot's twelve events would be needed to trip a threshold of twelve; if state
    // leaked between actors the scenario's other fourteen events would push it over sooner.
    val strict = config.copy(minEvents = 12, windowMillis = 5 * Minute)
    val bursts = detect(strict)

    bursts.map(_.actor) shouldBe List("bot")
    bursts.head.events shouldBe 12
  }
}
