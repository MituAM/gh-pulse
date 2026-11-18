package ghpulse.flink.live

import ghpulse.core.analytics.{ActivityStats, Burst, BurstConfig}
import ghpulse.core.cli.{Args, JobConfig}
import ghpulse.core.testkit.Reference
import ghpulse.flink.{FlinkSuite, RankedRepo}
import io.circe.parser.parse
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class LiveDashboardSpec extends FlinkSuite with Matchers {

  private def runJob(config: JobConfig, live: LiveOptions): io.circe.Json = {
    val env = environment()
    LiveDashboardJob.wire(scenarioStream(env), config, live)
    env.execute("live-dashboard-spec")
    val raw = new String(Files.readAllBytes(Paths.get(live.stateFile)), StandardCharsets.UTF_8)
    parse(raw).getOrElse(fail(s"state file is not JSON: $raw"))
  }

  private def configured(): (JobConfig, LiveOptions) = {
    val dir = Files.createTempDirectory("gh-pulse-live")
    val config = JobConfig
      .fromArgs(Seq("--input", "unused", "--window", "60", "--top", "5"))
      .getOrElse(fail("config"))
      .copy(burst = BurstConfig.default)
    val live = LiveOptions
      .fromArgs(Args.parse(Nil))
      .copy(stateFile = dir.resolve("state.json").toString, slideMillis = 10 * 60000L)
    (config, live)
  }

  test("the snapshot lists the same bursts the reference finds, with the repos they hit") {
    val (config, live) = configured()
    val json = runJob(config, live)

    val actors = json.hcursor
      .downField("bursts")
      .values
      .get
      .toList
      .flatMap(_.hcursor.get[String]("actor").toOption)
    actors.sorted shouldBe Reference.bursts(BurstConfig.default).map(_.actor).sorted

    val firstBurst = json.hcursor.downField("bursts").downN(0)
    firstBurst.downField("repos").values.get should not be empty
    firstBurst.get[Int]("events").toOption.get should be >= BurstConfig.default.minEvents
  }

  test("the snapshot carries a ranked trending board and the settings that produced it") {
    val (config, live) = configured()
    val json = runJob(config, live)

    val repos = json.hcursor.downField("trending").downField("repos").values.get.toList
    repos should not be empty
    repos.size should be <= config.topK
    repos.flatMap(_.hcursor.get[Int]("rank").toOption) shouldBe (1 to repos.size).toList
    json.hcursor.downField("settings").get[Int]("burstEvents").toOption shouldBe Some(10)
  }

  test("a newer window replaces the board and remembers the old one for movement") {
    def row(end: Long, rank: Int, repo: String, score: Double) =
      RankedRepo(end - 10, end, rank, repo, score, ActivityStats.empty)

    val d = Dashboard.empty
      .withRanked(row(100, 1, "a/a", 9))
      .withRanked(row(100, 2, "b/b", 5))
      .withRanked(row(200, 1, "b/b", 12))
      .withRanked(row(200, 2, "c/c", 3))
      .withRanked(row(100, 3, "stale/late", 1))

    d.top.map(_.repo) shouldBe Vector("b/b", "c/c")
    d.previous.keySet shouldBe Set("a/a", "b/b")

    val json = d.toJson(DashboardSettings(10, 1, 1, 1, trendingIncludesBots = false), 0L)
    val rows = json.hcursor.downField("trending").downField("repos")
    rows.downN(0).get[Int]("previousRank").toOption shouldBe Some(2)
    rows.downN(1).get[Boolean]("isNew").toOption shouldBe Some(true)
  }

  test("a burst lists the repositories it hit inside its own time range, busiest first") {
    val hits =
      Seq(Hit(1, "old/repo"), Hit(10, "a/a"), Hit(11, "b/b"), Hit(12, "b/b"), Hit(20, "late/x"))
    val burst = Burst(startedAt = 10, endedAt = 12, events = 3)
    LiveBursts.targets(hits, burst) shouldBe List(RepoHits("b/b", 2), RepoHits("a/a", 1))
  }

  test("the same burst replayed after a restart is listed once") {
    val b = LiveBurst("x[bot]", 12, 1L, 2L, Nil)
    Dashboard.empty.withBurst(b, 10).withBurst(b, 10).bursts should have size 1
    b.isBot shouldBe true
  }

  test("snapshots are replaced atomically and are valid UTF-8") {
    val dir = Files.createTempDirectory("gh-pulse-atomic")
    val target = dir.resolve("state.json")
    DashboardWriter.writeAtomically(target, """{"v":1}""")
    DashboardWriter.writeAtomically(target, """{"v":"ü"}""")
    new String(Files.readAllBytes(target), StandardCharsets.UTF_8) shouldBe """{"v":"ü"}"""
    Files.list(dir).count() shouldBe 1L
  }
}
