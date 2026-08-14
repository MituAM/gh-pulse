package ghpulse.spark

import ghpulse.core.domain.GitHubEvent
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/** Pairs of people who turn up in the same repositories.
  *
  * A self-join, which is the interesting part: joining the contribution table to itself on `repo`
  * would produce every ordered pair *including* each person with themselves, so the join condition
  * also demands `left.actor < right.actor`. That single predicate removes the self-pairs and keeps
  * only one of each mirrored pair, turning an n² join into the n(n-1)/2 rows actually wanted.
  */
object Collaboration {

  def contributions(events: Dataset[GitHubEvent]): DataFrame = {
    val spark = events.sparkSession
    import spark.implicits._

    events.select($"actor.login".as("actor"), $"repo.name".as("repo")).distinct()
  }

  /** @param minSharedRepos
    *   drop pairs that only ever met once; on a full day of GH Archive almost every pair has
    *   exactly one repository in common and the interesting signal is the repeat collaborators.
    */
  def pairs(events: Dataset[GitHubEvent], minSharedRepos: Int = 2): DataFrame = {
    val contributed = contributions(events)
    val left = contributed.as("l")
    val right = contributed.as("r")

    left
      .join(right, col("l.repo") === col("r.repo") && col("l.actor") < col("r.actor"))
      .groupBy(col("l.actor").as("actor_a"), col("r.actor").as("actor_b"))
      .agg(
        countDistinct(col("l.repo")).as("shared_repos"),
        sort_array(collect_set(col("l.repo"))).as("repos")
      )
      .filter(col("shared_repos") >= minSharedRepos)
      .orderBy(col("shared_repos").desc, col("actor_a").asc, col("actor_b").asc)
  }

  /** The repositories with the widest set of distinct contributors. */
  def mostContributors(events: Dataset[GitHubEvent], limit: Int): DataFrame =
    contributions(events)
      .groupBy(col("repo"))
      .agg(count("*").as("contributors"))
      .orderBy(col("contributors").desc, col("repo").asc)
      .limit(limit)
}
