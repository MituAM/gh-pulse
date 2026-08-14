package ghpulse.core.codec

import ghpulse.core.domain.{Actor, GitHubEvent, Payload, Repo}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/** A stand-in for the decoder a user of this project would write for their own corpus: four
  * comma-separated columns rather than GitHub's JSON. Also the worked example the README points to.
  */
object CsvEventDecoder extends EventDecoder {
  def decode(record: String): Either[String, GitHubEvent] = record.split(",", -1) match {
    case Array(id, timestamp, actor, repo) =>
      Try(timestamp.toLong).toEither.left
        .map(_ => s"'$timestamp' is not an epoch timestamp")
        .map(at =>
          GitHubEvent(
            id,
            "PushEvent",
            at,
            Actor(0L, actor),
            Repo(0L, repo),
            None,
            Payload.empty.copy(distinctSize = Some(1))
          )
        )
    case other => Left(s"expected 4 columns, got ${other.length}")
  }
}

object ExplodingDecoder extends EventDecoder {
  def decode(record: String): Either[String, GitHubEvent] = sys.error(s"boom on '$record'")
}

class NotADecoder

class EventDecoderSpec extends AnyFunSuite with Matchers {

  test("built-in decoders resolve by short name") {
    EventDecoder.resolve("gharchive") shouldBe Right(GitHubArchiveDecoder)
  }

  test("a user-supplied decoder object resolves by its class name") {
    val resolved = EventDecoder.resolve("ghpulse.core.codec.CsvEventDecoder")
    resolved shouldBe Right(CsvEventDecoder)

    val event = resolved.flatMap(_.decode("1,1705330800000,ada,acme/rocket"))
    event.map(_.repo.name) shouldBe Right("acme/rocket")
  }

  test("an unknown name explains itself and lists the built-ins") {
    val error = EventDecoder.resolve("com.example.Nope").left.getOrElse(fail("expected a failure"))
    error should include("com.example.Nope")
    error should include("gharchive")
  }

  test("a class that is not an EventDecoder is rejected rather than cast") {
    EventDecoder.resolve("ghpulse.core.codec.NotADecoder").isLeft shouldBe true
  }

  test("a decoder that throws is contained rather than allowed to fail the job") {
    val guarded = EventDecoder.guarded(ExplodingDecoder)
    val result = guarded.decode("anything").left.getOrElse(fail("expected a failure"))

    result should include("RuntimeException")
    result should include("boom on 'anything'")
  }

  test("guarding leaves a well-behaved decoder's answers untouched") {
    val record = "1,1705330800000,ada,acme/rocket"
    EventDecoder.guarded(CsvEventDecoder).decode(record) shouldBe CsvEventDecoder.decode(record)
    EventDecoder.guarded(CsvEventDecoder).decode("too,few").isLeft shouldBe true
  }
}
