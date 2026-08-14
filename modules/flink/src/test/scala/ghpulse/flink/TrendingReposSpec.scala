package ghpulse.flink

import ghpulse.core.analytics.ScoreWeights
import ghpulse.core.testkit.Fixtures.{Hour, T0}
import ghpulse.core.testkit.Reference
import org.scalatest.matchers.should.Matchers

class TrendingReposSpec extends FlinkSuite with Matchers {

  private val weights = ScoreWeights.default

  private def rankedByTumblingWindow(k: Int): List[RankedRepo] = {
    val env = environment()
    val activity = WindowedActivityStream.tumbling(scenarioStream(env), Hour, weights)
    collect(TrendingRepos.rank(activity, k))
  }

  test("ranks each window's repositories the way the reference does") {
    val ranked = rankedByTumblingWindow(k = 3)
      .groupBy(_.windowEnd)
      .map { case (end, rows) => end -> rows.sortBy(_.rank).map(_.repo) }

    val expected = Reference.trending(Hour, 3, weights).map { case (end, top) =>
      end -> top.map(_.repo)
    }

    ranked shouldBe expected
  }

  test("ranks start at one and run consecutively within a window") {
    val byWindow = rankedByTumblingWindow(k = 3).groupBy(_.windowEnd)

    byWindow.values.foreach { rows =>
      rows.map(_.rank).sorted shouldBe (1 to rows.size).toList
    }
  }

  test("k bounds each window independently") {
    val ranked = rankedByTumblingWindow(k = 1)

    ranked.map(_.rank).distinct shouldBe List(1)
    // The first hour has three active repositories and the second two; both yield one leader.
    ranked.map(_.windowEnd).sorted shouldBe List(T0 + Hour, T0 + 2 * Hour)
    ranked.map(_.repo) shouldBe List("acme/rocket", "acme/rocket")
  }

  test("scores and stats survive the second shuffle intact") {
    val leader = rankedByTumblingWindow(k = 3).find(r => r.windowEnd == T0 + Hour && r.rank == 1)

    leader.map(_.repo) shouldBe Some("acme/rocket")
    leader.map(_.score) shouldBe Some(25.0)
    leader.map(_.stats.commits) shouldBe Some(10L)
  }

  test("the end-to-end sliding leaderboard emits at most k rows per window") {
    val env = environment()
    val ranked = collect(TrendingRepos(scenarioStream(env), Hour, Hour / 2, k = 2, weights))

    ranked.groupBy(_.windowEnd).values.foreach(rows => rows.size should be <= 2)
    ranked.map(_.windowEnd).distinct.size should be > 2
  }
}
