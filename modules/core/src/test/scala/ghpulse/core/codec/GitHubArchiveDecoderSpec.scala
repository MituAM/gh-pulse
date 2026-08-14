package ghpulse.core.codec

import ghpulse.core.domain.EventKind
import ghpulse.core.testkit.Fixtures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class GitHubArchiveDecoderSpec extends AnyFunSuite with Matchers {

  private val decoder = GitHubArchiveDecoder

  test("decodes a push event including its commits") {
    val json =
      """{"id":"34834866568","type":"PushEvent",
        | "actor":{"id":1287731,"login":"NehalDamania"},
        | "repo":{"id":702006516,"name":"NehalDamania/becpl-uptime"},
        | "public":true,"created_at":"2024-01-15T15:00:02Z",
        | "payload":{"push_id":16645577295,"size":2,"distinct_size":2,"ref":"refs/heads/master",
        |   "commits":[{"sha":"e61029b","author":{"name":"Upptime Bot","email":"bot@example.com"},
        |               "message":"Update summary","distinct":true}]}}""".stripMargin

    val event = decoder.decode(json).getOrElse(fail("expected a decoded event"))

    event.id shouldBe "34834866568"
    event.kind shouldBe EventKind.Push
    event.createdAt shouldBe 1705330802000L
    event.actor.login shouldBe "NehalDamania"
    event.repo.name shouldBe "NehalDamania/becpl-uptime"
    event.repo.owner shouldBe "NehalDamania"
    event.payload.distinctSize shouldBe Some(2)
    event.payload.commits.map(_.sha) shouldBe List("e61029b")
    event.payload.commits.head.authorName shouldBe "Upptime Bot"
  }

  test("reads the nested pull request fields analytics depends on") {
    val json =
      """{"id":"1","type":"PullRequestEvent","actor":{"id":1,"login":"ada"},
        | "repo":{"id":2,"name":"acme/rocket"},"org":{"id":3,"login":"acme"},
        | "created_at":"2024-01-15T15:30:00Z",
        | "payload":{"action":"closed","number":73,
        |   "pull_request":{"merged":true,"additions":24,"deletions":23,"changed_files":2}}}""".stripMargin

    val event = decoder.decode(json).getOrElse(fail("expected a decoded event"))

    event.org shouldBe Some("acme")
    event.payload.action shouldBe Some("closed")
    event.payload.pullRequestNumber shouldBe Some(73)
    event.payload.merged shouldBe Some(true)
    event.payload.additions shouldBe Some(24)
    event.payload.changedFiles shouldBe Some(2)
  }

  test("an event type this project has never heard of decodes rather than fails") {
    val json =
      """{"id":"1","type":"SponsorshipEvent","actor":{"id":1,"login":"ada"},
        | "repo":{"id":2,"name":"acme/rocket"},"created_at":"2024-01-15T15:00:00Z",
        | "payload":{"whatever":{"nested":[1,2,3]}}}""".stripMargin

    val event = decoder.decode(json).getOrElse(fail("expected a decoded event"))
    event.kind shouldBe EventKind.Unknown("SponsorshipEvent")
  }

  test("absent, null and empty payloads all decode to an empty payload") {
    val bodies = List(
      """"payload":{}""",
      """"payload":null""",
      """"public":true"""
    )
    bodies.foreach { body =>
      val json =
        s"""{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"ada"},
           | "repo":{"id":2,"name":"acme/rocket"},"created_at":"2024-01-15T15:00:00Z",$body}""".stripMargin
      val event = decoder.decode(json).getOrElse(fail(s"expected a decoded event for $body"))
      event.payload.commits shouldBe empty
      event.payload.action shouldBe None
    }
  }

  test("accepts epoch millis in place of an ISO-8601 timestamp") {
    val json =
      """{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"ada"},
        | "repo":{"id":2,"name":"acme/rocket"},"created_at":1705330800000,"payload":{}}""".stripMargin

    decoder.decode(json).map(_.createdAt) shouldBe Right(1705330800000L)
  }

  test("reports rather than throws on records that cannot be used") {
    val rejected = List(
      "" -> "empty line",
      "{not json" -> "malformed json",
      """{"type":"WatchEvent"}""" -> "missing required fields",
      """{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"ada"},
        | "repo":{"id":2,"name":"acme/rocket"},"created_at":"never"}""".stripMargin -> "bad timestamp"
    )

    rejected.foreach { case (record, why) =>
      withClue(s"$why: ") {
        decoder.decode(record).isLeft shouldBe true
      }
    }
  }

  test("decodes every line of the committed GH Archive sample") {
    val source = Source.fromFile(Fixtures.sampleFile, "UTF-8")
    val results =
      try source.getLines().map(line => line -> decoder.decode(line)).toList
      finally source.close()

    val failures = results.collect { case (line, Left(error)) => s"$error in: ${line.take(120)}" }

    withClue(
      s"${failures.size} of ${results.size} records failed:\n${failures.take(5).mkString("\n")}\n"
    ) {
      failures shouldBe empty
    }
    results.size shouldBe 1335
    results.flatMap(_._2.toOption).map(_.kind).distinct.size should be > 10
  }
}
