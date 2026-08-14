package ghpulse.core.domain

/** The kind of activity an event represents.
  *
  * Modelled as a closed hierarchy so that analytics can pattern match exhaustively, with an open
  * `Unknown` case because GitHub adds event types over time and a public firehose must not fail on
  * one it has never seen.
  */
sealed abstract class EventKind(val wireName: String) extends Product with Serializable

object EventKind {
  case object Push extends EventKind("PushEvent")
  case object PullRequest extends EventKind("PullRequestEvent")
  case object PullRequestReview extends EventKind("PullRequestReviewEvent")
  case object PullRequestReviewComment extends EventKind("PullRequestReviewCommentEvent")
  case object Issues extends EventKind("IssuesEvent")
  case object IssueComment extends EventKind("IssueCommentEvent")
  case object CommitComment extends EventKind("CommitCommentEvent")
  case object Watch extends EventKind("WatchEvent")
  case object Fork extends EventKind("ForkEvent")
  case object Create extends EventKind("CreateEvent")
  case object Delete extends EventKind("DeleteEvent")
  case object Release extends EventKind("ReleaseEvent")
  case object Public extends EventKind("PublicEvent")
  case object Member extends EventKind("MemberEvent")
  case object Gollum extends EventKind("GollumEvent")

  final case class Unknown(name: String) extends EventKind(name)

  val known: List[EventKind] = List(
    Push,
    PullRequest,
    PullRequestReview,
    PullRequestReviewComment,
    Issues,
    IssueComment,
    CommitComment,
    Watch,
    Fork,
    Create,
    Delete,
    Release,
    Public,
    Member,
    Gollum
  )

  private val byWireName: Map[String, EventKind] = known.map(k => k.wireName -> k).toMap

  def fromWireName(name: String): EventKind = byWireName.getOrElse(name, Unknown(name))
}
