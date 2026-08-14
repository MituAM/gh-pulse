package ghpulse.core.domain

/** One public GitHub event, normalised to the fields the pipelines actually read.
  *
  * Every field is a primitive, an `Option`, a `List` or another case class. That restraint is
  * deliberate: it is exactly the shape Spark can derive an `Encoder` for and Flink a
  * `CaseClassSerializer` for, so a single type flows through both engines with no custom
  * serialization code. `kind` is a method rather than a field for the same reason — a sealed
  * hierarchy would defeat both derivations.
  *
  * @param createdAt
  *   epoch milliseconds; this is the event-time attribute the streaming jobs order on.
  */
final case class GitHubEvent(
    id: String,
    eventType: String,
    createdAt: Long,
    actor: Actor,
    repo: Repo,
    org: Option[String],
    payload: Payload
) {
  def kind: EventKind = EventKind.fromWireName(eventType)
}

final case class Actor(id: Long, login: String)

final case class Repo(id: Long, name: String) {

  /** `name` is always `owner/repository`, so the owner is everything up to the first slash. */
  def owner: String = name.takeWhile(_ != '/')
}

final case class PushCommit(
    sha: String,
    authorName: String,
    authorEmail: String,
    message: String,
    distinct: Boolean
)

/** The union of the payload fields used across event types.
  *
  * GitHub's payload schema is a different shape per event type. Flattening the interesting parts
  * into one optional-heavy record keeps the record encodable by both engines; `EventKind` is what
  * tells analytics which fields are meaningful for a given event.
  */
final case class Payload(
    action: Option[String],
    ref: Option[String],
    refType: Option[String],
    pushSize: Option[Int],
    distinctSize: Option[Int],
    commits: List[PushCommit],
    pullRequestNumber: Option[Int],
    merged: Option[Boolean],
    additions: Option[Int],
    deletions: Option[Int],
    changedFiles: Option[Int]
)

object Payload {
  val empty: Payload = Payload(None, None, None, None, None, Nil, None, None, None, None, None)
}
