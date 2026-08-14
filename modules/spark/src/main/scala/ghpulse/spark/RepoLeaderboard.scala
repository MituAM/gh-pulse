package ghpulse.spark

import ghpulse.core.analytics.{ActivityStats, RepoActivity, ScoreWeights, TopK}
import ghpulse.core.domain.GitHubEvent
import ghpulse.core.typeclass.Monoid
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/** Which repositories saw the most activity.
  *
  * Written twice on purpose. [[usingDataset]] is how this should be expressed in a modern codebase;
  * [[usingRdd]] is the same computation one layer down, where the shuffle behaviour is explicit.
  * `RepoLeaderboardSpec` asserts the two agree, so the RDD version is a demonstration that is
  * checked rather than a comment that claims something.
  */
object RepoLeaderboard {

  /** Typed aggregation: one `ActivityStats` per event, folded per repository.
    *
    * `reduceGroups` compiles to a map-side combine followed by a shuffle of partial aggregates, so
    * raw events never cross the network — the same reason the RDD version below uses
    * `aggregateByKey` instead of `groupByKey`.
    */
  def usingDataset(
      events: Dataset[GitHubEvent],
      k: Int,
      weights: ScoreWeights
  ): List[RepoActivity] = {
    val spark = events.sparkSession
    import spark.implicits._

    val combine = Monoid[ActivityStats].combine _

    val perRepo = events
      .groupByKey(_.repo.name)
      .mapValues(ActivityStats.of)
      .reduceGroups(combine)
      .map { case (repo, stats) => RepoActivity(repo, stats, weights) }

    perRepo.orderBy($"score".desc, $"repo".asc).limit(k).collect().toList
  }

  /** The same answer without the SQL engine.
    *
    * `aggregateByKey` folds within each partition before shuffling, and `treeAggregate` merges the
    * per-partition [[TopK]] results through a tree rather than dragging every repository back to
    * the driver. On a full GH Archive day that is the difference between shipping a few dozen rows
    * per partition and shipping millions.
    */
  def usingRdd(events: RDD[GitHubEvent], k: Int, weights: ScoreWeights): List[RepoActivity] = {
    val monoid = Monoid[ActivityStats]
    // Hoisted out of the closures below so Spark serialises the values, not their owning objects.
    implicit val ranking: Ordering[RepoActivity] = RepoActivity.ordering
    val scoring = weights

    val ranked = events
      .map(event => (event.repo.name, event))
      .aggregateByKey(monoid.empty)(
        seqOp = (stats, event) => monoid.combine(stats, ActivityStats.of(event)),
        combOp = monoid.combine
      )
      .map { case (repo, stats) => RepoActivity(repo, stats, scoring) }

    ranked
      .treeAggregate(TopK.empty[RepoActivity](k))(
        seqOp = (top, activity) => top.add(activity),
        combOp = (left, right) => left.merge(right)
      )
      .toList
  }

  /** Formats a leaderboard for the console. */
  def render(leaderboard: List[RepoActivity]): String = {
    val header =
      f"${"#"}%3s  ${"repository"}%-45s ${"score"}%9s ${"commits"}%8s ${"prs"}%5s ${"stars"}%6s"
    val rows = leaderboard.zipWithIndex.map { case (activity, index) =>
      f"${index + 1}%3d  ${activity.repo.take(45)}%-45s ${activity.score}%9.1f " +
        f"${activity.stats.commits}%8d ${activity.stats.pullRequests}%5d ${activity.stats.stars}%6d"
    }
    (header :: rows).mkString("\n")
  }
}
