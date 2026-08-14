package ghpulse.spark

import ghpulse.core.analytics.{ActivityStats, ScoreWeights, TimeBucket}
import ghpulse.core.domain.GitHubEvent
import ghpulse.core.typeclass.Monoid
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/** How each repository's activity moves from one window to the next.
  *
  * The per-event classification stays in the typed world, where the shared [[ActivityStats.of]]
  * rules apply; only the *shape* of the answer — running totals, period-over-period change, rank
  * within a period — is expressed in Spark SQL, which is what window functions are actually good
  * at. Rewriting the classification as a pile of `when(...)` expressions would have duplicated the
  * rules in a second dialect.
  */
object ActivityTimeline {

  /** One row per repository per window. */
  final case class Bucketed(repo: String, windowStart: Long, stats: ActivityStats)

  /** Buckets events and appends the derived columns.
    *
    * Columns added on top of the aggregates:
    *   - `score` — the weighted activity score for the window;
    *   - `rolling_score` — the same score summed over this window and the two before it;
    *   - `previous_score` / `score_delta` — the change since the repository's last active window,
    *     which is `null`/0 for its first;
    *   - `rank_in_window` — where the repository placed among all repositories in that window.
    */
  def compute(
      events: Dataset[GitHubEvent],
      windowMillis: Long,
      weights: ScoreWeights
  ): DataFrame = {
    val spark = events.sparkSession
    import spark.implicits._

    val size = windowMillis
    val scoring = weights

    val bucketed = events
      .map(event =>
        Bucketed(event.repo.name, TimeBucket.floor(event.createdAt, size), ActivityStats.of(event))
      )
      .groupByKey(row => (row.repo, row.windowStart))
      .mapValues(_.stats)
      .reduceGroups(Monoid[ActivityStats].combine _)
      .map { case ((repo, windowStart), stats) => Bucketed(repo, windowStart, stats) }
      .toDF()
      .withColumn("score", scoreColumn(scoring))

    // Ordered by time within a repository: the frame for running totals and period-over-period.
    val overRepo = Window.partitionBy($"repo").orderBy($"windowStart")
    // Ordered by score within a window: the frame for ranking repositories against each other.
    val overWindow = Window.partitionBy($"windowStart").orderBy($"score".desc, $"repo".asc)

    bucketed
      .withColumn("rolling_score", sum($"score").over(overRepo.rowsBetween(-2, Window.currentRow)))
      .withColumn("previous_score", lag($"score", 1).over(overRepo))
      .withColumn("score_delta", $"score" - coalesce($"previous_score", lit(0.0)))
      .withColumn("rank_in_window", rank().over(overWindow))
      .withColumn("window_label", windowLabelColumn($"windowStart"))
      .orderBy($"windowStart".asc, $"rank_in_window".asc)
  }

  /** The busiest repositories of each window, as a readable table. */
  def leadersPerWindow(timeline: DataFrame, perWindow: Int): DataFrame =
    timeline
      .filter(col("rank_in_window") <= perWindow)
      .select("window_label", "rank_in_window", "repo", "score", "score_delta", "rolling_score")

  /** The weighted score, expressed over the nested `stats` struct. */
  private def scoreColumn(w: ScoreWeights): Column =
    lit(w.commit) * col("stats.commits") +
      lit(w.pullRequest) * col("stats.pullRequests") +
      lit(w.mergedPullRequest) * col("stats.mergedPullRequests") +
      lit(w.issue) * col("stats.issues") +
      lit(w.comment) * col("stats.comments") +
      lit(w.star) * col("stats.stars") +
      lit(w.fork) * col("stats.forks")

  private def windowLabelColumn(windowStart: Column): Column =
    date_format((windowStart / 1000).cast("timestamp"), "yyyy-MM-dd'T'HH:mm'Z'")
}
