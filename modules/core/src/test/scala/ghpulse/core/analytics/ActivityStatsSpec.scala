package ghpulse.core.analytics

import ghpulse.core.domain.Payload
import ghpulse.core.testkit.Fixtures
import ghpulse.core.testkit.Fixtures.{Minute, T0}
import ghpulse.core.typeclass.Monoid
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ActivityStatsSpec extends AnyFunSuite with Matchers {

  test("a push contributes its distinct commit count") {
    val stats = ActivityStats.of(Fixtures.push("e1", T0, "ada", "acme/rocket", commits = 3))
    stats.pushes shouldBe 1L
    stats.commits shouldBe 3L
    stats.events shouldBe 1L
  }

  test("a push falls back to size, then to the commit list, when distinct_size is absent") {
    val base = Fixtures.push("e1", T0, "ada", "acme/rocket", commits = 4)

    val withoutDistinct = base.copy(payload = base.payload.copy(distinctSize = None))
    ActivityStats.of(withoutDistinct).commits shouldBe 4L // from pushSize

    val withNeither = base.copy(payload = base.payload.copy(distinctSize = None, pushSize = None))
    ActivityStats.of(withNeither).commits shouldBe 4L // from the commits array
  }

  test("a push in the trimmed post-2025 format counts as one commit, not zero") {
    val trimmed = Fixtures
      .push("e1", T0, "ada", "acme/rocket", commits = 4)
      .copy(payload = Payload.empty.copy(ref = Some("refs/heads/main")))

    val stats = ActivityStats.of(trimmed)
    stats.pushes shouldBe 1L
    stats.commits shouldBe 1L
  }

  test("the post-2025 'merged' action counts as a merge, without line counts") {
    val merged = Fixtures
      .pullRequest("e1", T0, "ada", "acme/rocket", "opened")
      .copy(payload = Payload.empty.copy(action = Some("merged"), pullRequestNumber = Some(7)))

    val stats = ActivityStats.of(merged)
    stats.pullRequests shouldBe 1L
    stats.mergedPullRequests shouldBe 1L
    stats.linesAdded shouldBe 0L
  }

  test("only a merged pull request contributes line counts") {
    val opened = Fixtures.pullRequest("e1", T0, "ada", "acme/rocket", "opened", additions = 100)
    ActivityStats.of(opened).pullRequests shouldBe 1L
    ActivityStats.of(opened).mergedPullRequests shouldBe 0L
    ActivityStats.of(opened).linesAdded shouldBe 0L

    val merged = Fixtures.pullRequest(
      "e2",
      T0,
      "ada",
      "acme/rocket",
      "closed",
      merged = true,
      additions = 100,
      deletions = 25
    )
    ActivityStats.of(merged).mergedPullRequests shouldBe 1L
    ActivityStats.of(merged).linesAdded shouldBe 100L
    ActivityStats.of(merged).linesDeleted shouldBe 25L
  }

  test("a pull request closed without merging contributes no lines") {
    val abandoned = Fixtures.pullRequest(
      "e1",
      T0,
      "ada",
      "acme/rocket",
      "closed",
      merged = false,
      additions = 999
    )
    ActivityStats.of(abandoned).mergedPullRequests shouldBe 0L
    ActivityStats.of(abandoned).linesAdded shouldBe 0L
  }

  test("a WatchEvent counts as a star") {
    ActivityStats.of(Fixtures.star("e1", T0, "grace", "acme/rocket")).stars shouldBe 1L
  }

  test("an unrecognised event type still counts towards the event total") {
    val exotic = Fixtures.event("e1", "SponsorshipEvent", T0, "grace", "acme/rocket")
    val stats = ActivityStats.of(exotic)
    stats.events shouldBe 1L
    stats shouldBe ActivityStats.empty.copy(events = 1L)
  }

  test("a negative commit count from malformed input cannot drag the total down") {
    val corrupt = Fixtures
      .push("e1", T0, "ada", "acme/rocket")
      .copy(payload = Payload.empty.copy(distinctSize = Some(-5)))
    ActivityStats.of(corrupt).commits shouldBe 0L
  }

  test("scoring is a weighted sum and responds to reweighting") {
    val stats = ActivityStats.empty.copy(commits = 10, stars = 2, forks = 1)
    val defaults = ScoreWeights.default

    stats.score(defaults) shouldBe (10 * defaults.commit + 2 * defaults.star + defaults.fork)

    val commitsOnly = defaults.copy(star = 0.0, fork = 0.0)
    stats.score(commitsOnly) shouldBe 10.0
  }

  test("the scenario aggregates to the counts the other engines are asserted against") {
    val rocket = Fixtures.scenario.filter(_.repo.name == "acme/rocket")
    val stats = Monoid[ActivityStats].combineAll(rocket.map(ActivityStats.of))

    stats.events shouldBe 9L
    stats.pushes shouldBe 4L
    stats.commits shouldBe 11L // 3 + 2 + 5 + 1
    stats.pullRequests shouldBe 2L
    stats.mergedPullRequests shouldBe 1L
    stats.stars shouldBe 2L
    stats.forks shouldBe 1L
    stats.linesAdded shouldBe 120L
    stats.linesDeleted shouldBe 40L
  }

  test("splitting the scenario by hour and recombining gives the whole-scenario total") {
    val all = Monoid[ActivityStats].combineAll(Fixtures.scenario.map(ActivityStats.of))
    val byHour = Fixtures.scenario
      .groupBy(e => TimeBucket.floor(e.createdAt, TimeBucket.Hour))
      .values
      .map(events => Monoid[ActivityStats].combineAll(events.map(ActivityStats.of)))

    Monoid[ActivityStats].combineAll(byHour) shouldBe all
    all.events shouldBe (9L + 5L + 12L)
  }

  test("the burst window sits inside a single hour bucket") {
    val botTimes = Fixtures.scenario.filter(_.actor.login == "bot").map(_.createdAt)
    botTimes.map(TimeBucket.floor(_, TimeBucket.Hour)).distinct should have size 1
    (botTimes.max - botTimes.min) should be < (5 * Minute)
  }
}
