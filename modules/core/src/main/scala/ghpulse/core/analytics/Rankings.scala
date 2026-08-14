package ghpulse.core.analytics

/** A repository's aggregated activity and the score derived from it. */
final case class RepoActivity(repo: String, stats: ActivityStats, score: Double)

object RepoActivity extends Serializable {

  def apply(repo: String, stats: ActivityStats, weights: ScoreWeights): RepoActivity =
    RepoActivity(repo, stats, weights.applyTo(stats))

  /** Ranks by score, breaking ties on repository name.
    *
    * The tiebreak is not cosmetic. [[TopK]] is only associative under a total order, and repository
    * names are unique, so adding the name is what makes "top 20" come out identical no matter how
    * Spark partitioned the input or how Flink sliced the window.
    */
  implicit val ordering: Ordering[RepoActivity] =
    Ordering.by[RepoActivity, (Double, String)](a => (a.score, a.repo))
}

/** A repository's activity within one closed time window. */
final case class WindowedActivity(
    windowStart: Long,
    windowEnd: Long,
    repo: String,
    stats: ActivityStats,
    score: Double
) {
  def toRepoActivity: RepoActivity = RepoActivity(repo, stats, score)
  def windowLabel: String = TimeBucket.formatMinute(windowStart)
}

/** A burst attributed to the actor that produced it. */
final case class ActorBurst(actor: String, burst: Burst) {
  def startedAt: Long = burst.startedAt
  def endedAt: Long = burst.endedAt
  def events: Int = burst.events
}

object ActorBurst extends Serializable {

  /** Chronological, then by actor, so batch and streaming results can be compared element-wise. */
  implicit val ordering: Ordering[ActorBurst] =
    Ordering.by[ActorBurst, (Long, Long, String)](b => (b.endedAt, b.startedAt, b.actor))
}
