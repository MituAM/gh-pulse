package ghpulse.core.analytics

import ghpulse.core.testkit.Fixtures.{Minute, T0}
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BurstDetectorSpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {

  private val config =
    BurstConfig(minEvents = 5, windowMillis = 2 * Minute, cooldownMillis = 10 * Minute)

  private def at(minutes: Double): Long = T0 + (minutes * Minute).toLong

  test("reports nothing while activity stays under the threshold") {
    val sparse = (0 until 20).map(n => at(n * 5.0))
    BurstDetector.detect(sparse, config) shouldBe empty
  }

  test("reports on the event that completes the window") {
    val dense = (0 until 5).map(n => at(n * 0.25))
    val bursts = BurstDetector.detect(dense, config)

    bursts should have size 1
    bursts.head.events shouldBe 5
    bursts.head.startedAt shouldBe dense.head
    bursts.head.endedAt shouldBe dense.last
  }

  test("events spread wider than the window never accumulate into one") {
    val justTooSlow = (0 until 20).map(n => at(n * 0.51)) // 5 events span slightly over 2 minutes
    BurstDetector.detect(justTooSlow, config) shouldBe empty
  }

  test("the cooldown collapses one sustained burst into a single report") {
    val sustained = (0 until 60).map(n => at(n * 0.1)) // six minutes of constant activity
    val bursts = BurstDetector.detect(sustained, config)

    bursts should have size 1
    bursts.head.endedAt shouldBe at(4 * 0.1)
  }

  test("activity resuming after the cooldown reports again") {
    val first = (0 until 5).map(n => at(n * 0.25))
    val later = (0 until 5).map(n => at(15.0 + n * 0.25))
    BurstDetector.detect(first ++ later, config) should have size 2
  }

  test("reports exactly when some window holds enough events") {
    val timestamp = Gen.chooseNum(0L, 20L).map(n => at(n * 0.4))

    forAll(Gen.listOf(timestamp).map(_.sorted)) { timestamps =>
      val someWindowIsFull = timestamps.exists { end =>
        timestamps.count(t => t > end - config.windowMillis && t <= end) >= config.minEvents
      }
      BurstDetector.detect(timestamps, config).nonEmpty shouldBe someWindowIsFull
    }
  }

  test("out-of-order arrival is tolerated, though it shifts when a burst is noticed") {
    val timestamps = (0 until 12).map(n => at(n * 0.25)).toList
    val shuffled = List(11, 3, 7, 0, 9, 1, 5, 2, 10, 4, 8, 6).map(timestamps)

    // The rule always measures the window ending at the latest timestamp *seen so far*, so a
    // record that arrives early prunes older ones and the report lands on a different event.
    // Detecting the burst at all is what must survive reordering; Flink's watermarks are what
    // narrow the difference in the streaming job.
    BurstDetector.detect(shuffled, config) should not be empty
    all(BurstDetector.detect(shuffled, config).map(b => b.endedAt - b.startedAt)) should
      be <= config.windowMillis
  }

  test("state carries forward so streaming one event at a time matches a batch fold") {
    val timestamps = (0 until 30).map(n => at(n * 0.2)).toList

    val streamed = timestamps
      .foldLeft((BurstState.empty, List.empty[Burst])) { case ((state, acc), ts) =>
        val (next, burst) = BurstDetector.step(state, ts, config)
        (next, burst.fold(acc)(_ :: acc))
      }
      ._2
      .reverse

    streamed shouldBe BurstDetector.detect(timestamps, config)
  }

  test("state only ever holds the timestamps inside the current window") {
    val timestamps = (0 until 50).map(n => at(n * 0.3))
    val finalState = timestamps.foldLeft(BurstState.empty) { (state, ts) =>
      BurstDetector.step(state, ts, config)._1
    }

    finalState.recent.size should be <= 7
    all(finalState.recent.map(timestamps.last - _)) should be < config.windowMillis
  }

  test("a threshold of one or a non-positive window is rejected") {
    an[IllegalArgumentException] should be thrownBy BurstConfig(1, 1000L, 0L)
    an[IllegalArgumentException] should be thrownBy BurstConfig(5, 0L, 0L)
  }
}
