package ghpulse.core.codec

import ghpulse.core.domain._
import io.circe.{ACursor, Decoder, HCursor}

import java.time.Instant
import scala.util.control.NonFatal

/** Reads the JSON that GH Archive and the GitHub Events API emit.
  *
  * Written by hand rather than derived, because the wire format is a moving target that this
  * project does not control. Unknown event types, absent payload fields and payload shapes that
  * have changed since this was written all decode successfully into whatever subset is
  * recognisable, and only a structurally broken record fails.
  */
object GitHubArchiveDecoder extends EventDecoder {

  def decode(record: String): Either[String, GitHubEvent] =
    io.circe.parser.decode[GitHubEvent](record)(eventDecoder).left.map(_.getMessage)

  private val actorDecoder: Decoder[Actor] = Decoder.instance { c =>
    for {
      id <- c.get[Long]("id")
      login <- c.get[String]("login")
    } yield Actor(id, login)
  }

  private val repoDecoder: Decoder[Repo] = Decoder.instance { c =>
    for {
      id <- c.get[Long]("id")
      name <- c.get[String]("name")
    } yield Repo(id, name)
  }

  private implicit val commitDecoder: Decoder[PushCommit] = Decoder.instance { c =>
    val author = c.downField("author")
    for {
      sha <- c.get[String]("sha")
      name <- author.get[Option[String]]("name")
      email <- author.get[Option[String]]("email")
      message <- c.get[Option[String]]("message")
      distinct <- c.get[Option[Boolean]]("distinct")
    } yield PushCommit(
      sha = sha,
      authorName = name.getOrElse(""),
      authorEmail = email.getOrElse(""),
      message = message.getOrElse(""),
      distinct = distinct.getOrElse(true)
    )
  }

  private val payloadDecoder: Decoder[Payload] = Decoder.instance { c =>
    val pr: ACursor = c.downField("pull_request")
    for {
      action <- c.get[Option[String]]("action")
      ref <- c.get[Option[String]]("ref")
      refType <- c.get[Option[String]]("ref_type")
      pushSize <- c.get[Option[Int]]("size")
      distinctSize <- c.get[Option[Int]]("distinct_size")
      commits <- c.get[Option[List[PushCommit]]]("commits")
      number <- c.get[Option[Int]]("number")
      merged <- pr.get[Option[Boolean]]("merged")
      additions <- pr.get[Option[Int]]("additions")
      deletions <- pr.get[Option[Int]]("deletions")
      changedFiles <- pr.get[Option[Int]]("changed_files")
    } yield Payload(
      action = action,
      ref = ref,
      refType = refType,
      pushSize = pushSize,
      distinctSize = distinctSize,
      commits = commits.getOrElse(Nil),
      pullRequestNumber = number,
      merged = merged,
      additions = additions,
      deletions = deletions,
      changedFiles = changedFiles
    )
  }

  /** Accepts ISO-8601 (what GitHub sends) or raw epoch millis (convenient for hand-written data).
    */
  private val timestampDecoder: Decoder[Long] =
    Decoder[Long].or(Decoder[String].emap { raw =>
      try Right(Instant.parse(raw).toEpochMilli)
      catch { case NonFatal(_) => Left(s"not an ISO-8601 instant: '$raw'") }
    })

  private val eventDecoder: Decoder[GitHubEvent] = Decoder.instance { c: HCursor =>
    for {
      id <- c.get[String]("id")
      eventType <- c.get[String]("type")
      createdAt <- c.get[Long]("created_at")(timestampDecoder)
      actor <- c.get[Actor]("actor")(actorDecoder)
      repo <- c.get[Repo]("repo")(repoDecoder)
      org <- c.downField("org").get[Option[String]]("login")
      // An event with no payload at all is still a usable event; treat it as an empty one.
      payload <- c.get[Option[Payload]]("payload")(Decoder.decodeOption(payloadDecoder))
    } yield GitHubEvent(
      id = id,
      eventType = eventType,
      createdAt = createdAt,
      actor = actor,
      repo = repo,
      org = org,
      payload = payload.getOrElse(Payload.empty)
    )
  }
}
