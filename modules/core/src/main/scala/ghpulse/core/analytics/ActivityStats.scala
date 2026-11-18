package ghpulse.core.analytics

import ghpulse.core.domain.{EventKind, GitHubEvent}
import ghpulse.core.typeclass.Monoid

/** Additive summary of a set of events.
  *
  * Every field is a count, so the whole record is a monoid under field-wise addition. That is the
  * single abstraction both engines aggregate with: Spark folds it with `reduceByKey`, Flink with an
  * incremental `AggregateFunction`.
  */
final case class ActivityStats(
    events: Long,
    pushes: Long,
    commits: Long,
    pullRequests: Long,
    mergedPullRequests: Long,
    issues: Long,
    comments: Long,
    stars: Long,
    forks: Long,
    linesAdded: Long,
    linesDeleted: Long
) {
  def score(weights: ScoreWeights = ScoreWeights.default): Double = weights.applyTo(this)
}

object ActivityStats extends Serializable {

  val empty: ActivityStats = ActivityStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  /** What a push counts for when its payload carries no commit information (the post-2025 format):
    * a push lands at least one commit, and counting it as zero would make pushes invisible to the
    * score.
    */
  private val TrimmedPushCommits: Int = 1

  /** Projects a single event onto the counters it contributes to.
    *
    * Judgement calls worth knowing about:
    *   - a push contributes `distinct_size` commits, not `size`, so commits that were already
    *     counted on another branch are not counted twice;
    *   - a pull request only contributes line counts when it is actually merged, because GitHub
    *     emits a `PullRequestEvent` on open, synchronize and close alike and the diff size is
    *     repeated on every one of them;
    *   - since 2025-10-07 GitHub strips commit counts from `PushEvent` and diff details from
    *     `PullRequestEvent` (in the Events API, and therefore in GH Archive too). A push with no
    *     count information at all is counted as one commit rather than zero, and a merge is
    *     recognised by the newer `action: "merged"` as well as the older `closed` + `merged`.
    */
  def of(event: GitHubEvent): ActivityStats = {
    val p = event.payload
    val base = empty.copy(events = 1L)

    event.kind match {
      case EventKind.Push =>
        val commitCount = p.distinctSize
          .orElse(p.pushSize)
          .getOrElse(if (p.commits.nonEmpty) p.commits.size else TrimmedPushCommits)
        base.copy(pushes = 1L, commits = commitCount.toLong.max(0L))

      case EventKind.PullRequest =>
        val wasMerged =
          p.action.contains("merged") || (p.action.contains("closed") && p.merged.contains(true))
        base.copy(
          pullRequests = 1L,
          mergedPullRequests = if (wasMerged) 1L else 0L,
          linesAdded = if (wasMerged) p.additions.getOrElse(0).toLong else 0L,
          linesDeleted = if (wasMerged) p.deletions.getOrElse(0).toLong else 0L
        )

      case EventKind.Issues =>
        base.copy(issues = 1L)

      case EventKind.IssueComment | EventKind.CommitComment | EventKind.PullRequestReviewComment |
          EventKind.PullRequestReview =>
        base.copy(comments = 1L)

      // A WatchEvent is GitHub's name for starring a repository; nobody has "watched" since 2012.
      case EventKind.Watch =>
        base.copy(stars = 1L)

      case EventKind.Fork =>
        base.copy(forks = 1L)

      case _ =>
        base
    }
  }

  implicit val monoid: Monoid[ActivityStats] = Monoid.instance(empty) { (a, b) =>
    ActivityStats(
      events = a.events + b.events,
      pushes = a.pushes + b.pushes,
      commits = a.commits + b.commits,
      pullRequests = a.pullRequests + b.pullRequests,
      mergedPullRequests = a.mergedPullRequests + b.mergedPullRequests,
      issues = a.issues + b.issues,
      comments = a.comments + b.comments,
      stars = a.stars + b.stars,
      forks = a.forks + b.forks,
      linesAdded = a.linesAdded + b.linesAdded,
      linesDeleted = a.linesDeleted + b.linesDeleted
    )
  }
}

/** How much each kind of contribution counts towards a repository's activity score.
  *
  * Scoring is a policy, not a fact, so it lives in data rather than in a hard-coded formula: the
  * defaults reward things that are expensive to fake (a merged pull request) over things that are
  * cheap (a star), and every job accepts overrides from the command line.
  */
final case class ScoreWeights(
    commit: Double,
    pullRequest: Double,
    mergedPullRequest: Double,
    issue: Double,
    comment: Double,
    star: Double,
    fork: Double
) {
  def applyTo(s: ActivityStats): Double =
    commit * s.commits +
      pullRequest * s.pullRequests +
      mergedPullRequest * s.mergedPullRequests +
      issue * s.issues +
      comment * s.comments +
      star * s.stars +
      fork * s.forks
}

object ScoreWeights {
  val default: ScoreWeights = ScoreWeights(
    commit = 1.0,
    pullRequest = 2.0,
    mergedPullRequest = 5.0,
    issue = 1.5,
    comment = 0.5,
    star = 3.0,
    fork = 4.0
  )
}
