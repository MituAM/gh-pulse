package ghpulse.spark

import ghpulse.core.codec.{CsvEventDecoder, GitHubArchiveDecoder}
import ghpulse.core.testkit.Fixtures
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class EventSourceSpec extends SparkSuite with Matchers {

  private def writeLines(name: String, lines: Seq[String]): String = {
    val file: Path = Files.createTempDirectory("gh-pulse").resolve(name)
    Files.write(file, String.join("\n", lines: _*).getBytes(StandardCharsets.UTF_8))
    file.toString
  }

  test("reads the committed GH Archive sample end to end") {
    val (events, report) =
      EventSource.readAndReport(spark, Fixtures.sampleFile.toString, GitHubArchiveDecoder)

    report.accepted shouldBe 1335L
    report.rejected shouldBe 0L
    report.rejectionRate shouldBe 0.0

    events.select("repo").distinct().count() should be > 100L
    events.filter(_.kind.wireName == "PushEvent").count() should be > 500L
  }

  test("counts unreadable records instead of failing the job") {
    val good = """{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"ada"},
                 |"repo":{"id":2,"name":"acme/rocket"},"created_at":"2024-01-15T15:00:00Z"}""".stripMargin
      .replace("\n", "")
    val path = writeLines("mixed.jsonl", Seq(good, "{ not json", "", good))

    val (events, report) = EventSource.readAndReport(spark, path, GitHubArchiveDecoder)

    events.count() shouldBe 2L
    report.accepted shouldBe 2L
    report.rejected shouldBe 2L
    report.rejectionRate shouldBe 0.5
  }

  test("a user-supplied decoder swaps in without touching the pipeline") {
    val path = writeLines(
      "events.csv",
      Seq(
        s"1,${Fixtures.T0},ada,acme/rocket",
        s"2,${Fixtures.T0 + 1000},grace,acme/rocket",
        "3,not-a-number,broken,row"
      )
    )

    val (events, report) = EventSource.readAndReport(spark, path, CsvEventDecoder)

    report.accepted shouldBe 2L
    report.rejected shouldBe 1L

    // And the analytics downstream are none the wiser about where the data came from.
    val leaderboard =
      RepoLeaderboard.usingDataset(events, 5, ghpulse.core.analytics.ScoreWeights.default)
    leaderboard.map(_.repo) shouldBe List("acme/rocket")
    leaderboard.head.stats.commits shouldBe 2L
  }

  test("the events survive a round trip through Spark's encoders unchanged") {
    import spark.implicits._
    val original = Fixtures.scenario
    spark.createDataset(original).collect().toList should contain theSameElementsAs original
  }
}
