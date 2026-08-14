package ghpulse.spark

import ghpulse.core.analytics.ScoreWeights
import ghpulse.core.testkit.Reference
import org.scalatest.matchers.should.Matchers

class RepoLeaderboardSpec extends SparkSuite with Matchers {

  private val weights = ScoreWeights.default

  test("the Dataset implementation matches the single-threaded reference") {
    RepoLeaderboard.usingDataset(scenarioEvents(), 10, weights) shouldBe Reference.leaderboard(10)
  }

  test("the RDD implementation matches the same reference") {
    RepoLeaderboard.usingRdd(scenarioEvents().rdd, 10, weights) shouldBe Reference.leaderboard(10)
  }

  test("the two implementations agree however the input is partitioned") {
    Seq(1, 2, 5, 11).foreach { partitions =>
      val events = scenarioEvents(partitions)
      withClue(s"with $partitions partitions: ") {
        RepoLeaderboard.usingRdd(events.rdd, 3, weights) shouldBe
          RepoLeaderboard.usingDataset(events, 3, weights)
      }
    }
  }

  test("ranks the scenario's repositories in the expected order") {
    val top = RepoLeaderboard.usingDataset(scenarioEvents(), 10, weights)

    top.map(_.repo) shouldBe List("acme/rocket", "acme/telemetry", "acme/probe")
    top.map(_.score) shouldBe List(30.0, 12.0, 6.0)
    top.head.stats.commits shouldBe 11L
    top.head.stats.mergedPullRequests shouldBe 1L
  }

  test("k bounds the result without changing the ranking") {
    val top1 = RepoLeaderboard.usingDataset(scenarioEvents(), 1, weights)
    top1 should have size 1
    top1 shouldBe Reference.leaderboard(1)
  }

  test("reweighting changes the leader") {
    // Score only stars, and the repository with the most commits is no longer interesting.
    val starsOnly = ScoreWeights(0, 0, 0, 0, 0, star = 1.0, fork = 0)
    val top = RepoLeaderboard.usingDataset(scenarioEvents(), 3, starsOnly)

    top.head.repo shouldBe "acme/rocket"
    top.head.score shouldBe 2.0
    top.map(_.score) shouldBe List(2.0, 0.0, 0.0)
  }

  test("renders a table with one line per repository plus a header") {
    val rendered =
      RepoLeaderboard.render(RepoLeaderboard.usingDataset(scenarioEvents(), 3, weights))
    val lines = rendered.split("\n")

    lines should have size 4
    lines.head should include("repository")
    lines(1) should include("acme/rocket")
  }
}
