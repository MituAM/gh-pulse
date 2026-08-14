package ghpulse.core.testkit

import ghpulse.core.analytics._
import ghpulse.core.domain.GitHubEvent
import ghpulse.core.typeclass.Monoid

/** The answers, worked out in ordinary single-threaded Scala.
  *
  * This is the pivot of the whole test strategy. The Spark suites assert that a distributed batch
  * job reproduces these values and the Flink suites assert that a windowed, stateful streaming job
  * reproduces the same ones, so "batch and streaming agree" is checked against a fixed, readable
  * definition rather than by comparing two complicated things to each other. When an engine
  * disagrees, this says which one is wrong.
  *
  * Nothing here touches Spark or Flink, and it does not reuse either engine's aggregation path —
  * only the domain rules in `core.analytics`, which have their own tests.
  */
object Reference {

  private val stats = Monoid[ActivityStats]

  def leaderboard(
      k: Int,
      weights: ScoreWeights = ScoreWeights.default,
      events: List[GitHubEvent] = Fixtures.scenario
  ): List[RepoActivity] =
    events
      .groupBy(_.repo.name)
      .map { case (repo, group) =>
        RepoActivity(repo, stats.combineAll(group.map(ActivityStats.of)), weights)
      }
      .toList
      .sorted(RepoActivity.ordering.reverse)
      .take(k)

  /** Tumbling windows aligned to the epoch, which is where both engines put their boundaries. */
  def windows(
      windowMillis: Long,
      weights: ScoreWeights = ScoreWeights.default,
      events: List[GitHubEvent] = Fixtures.scenario
  ): List[WindowedActivity] =
    events
      .groupBy(event => (event.repo.name, TimeBucket.floor(event.createdAt, windowMillis)))
      .map { case ((repo, start), group) =>
        val aggregate = stats.combineAll(group.map(ActivityStats.of))
        WindowedActivity(start, start + windowMillis, repo, aggregate, weights.applyTo(aggregate))
      }
      .toList
      .sortBy(window => (window.windowStart, -window.score, window.repo))

  /** The top `k` of each window, which is what a streaming leaderboard emits. */
  def trending(
      windowMillis: Long,
      k: Int,
      weights: ScoreWeights = ScoreWeights.default,
      events: List[GitHubEvent] = Fixtures.scenario
  ): Map[Long, List[RepoActivity]] =
    windows(windowMillis, weights, events)
      .groupBy(_.windowEnd)
      .map { case (end, activity) => end -> TopK.from(k, activity.map(_.toRepoActivity)).toList }

  def bursts(
      config: BurstConfig = BurstConfig.default,
      events: List[GitHubEvent] = Fixtures.scenario
  ): List[ActorBurst] =
    events
      .groupBy(_.actor.login)
      .toList
      .flatMap { case (actor, group) =>
        BurstDetector.detect(group.map(_.createdAt).sorted, config).map(ActorBurst(actor, _))
      }
      .sorted(ActorBurst.ordering)
}
