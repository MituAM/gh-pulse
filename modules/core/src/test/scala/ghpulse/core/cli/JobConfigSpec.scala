package ghpulse.core.cli

import ghpulse.core.analytics.ScoreWeights
import ghpulse.core.codec.GitHubArchiveDecoder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArgsSpec extends AnyFunSuite with Matchers {

  test("reads --key value pairs") {
    val args = Args.parse(Seq("--input", "data/events.jsonl", "--top", "5"))
    args.get("input") shouldBe Some("data/events.jsonl")
    args.int("top", 20) shouldBe 5
  }

  test("a key with no value is a boolean flag") {
    Args.parse(Seq("--verbose", "--top", "5")).bool("verbose", default = false) shouldBe true
    Args.parse(Seq("--top", "5", "--verbose")).bool("verbose", default = false) shouldBe true
  }

  test("missing keys fall back to the default") {
    Args.parse(Seq.empty).int("top", 20) shouldBe 20
    Args.parse(Seq.empty).str("decoder", "gharchive") shouldBe "gharchive"
  }

  test("a value of the wrong type names the offending option") {
    val error =
      the[IllegalArgumentException] thrownBy Args.parse(Seq("--top", "many")).int("top", 20)
    error.getMessage should include("--top")
    error.getMessage should include("many")
  }
}

class JobConfigSpec extends AnyFunSuite with Matchers {

  test("a minimal command line fills in every default") {
    val config = JobConfig
      .fromArgs(Seq("--input", "data/sample/events.jsonl"))
      .getOrElse(fail("expected a valid configuration"))

    config.input shouldBe "data/sample/events.jsonl"
    config.output shouldBe None
    config.decoder shouldBe GitHubArchiveDecoder
    config.topK shouldBe 20
    config.weights shouldBe ScoreWeights.default
    config.windowMillis shouldBe 3600000L
    config.maxOutOfOrdernessMillis shouldBe 300000L
    config.burst.minEvents shouldBe 10
    config.burst.windowMillis shouldBe 300000L
  }

  test("durations are given in the units the help text advertises") {
    val config = JobConfig
      .fromArgs(
        Seq("--input", "x", "--window", "15", "--burst-window", "2", "--out-of-orderness", "30")
      )
      .getOrElse(fail("expected a valid configuration"))

    config.windowMillis shouldBe 15 * 60000L
    config.burst.windowMillis shouldBe 2 * 60000L
    config.maxOutOfOrdernessMillis shouldBe 30000L
  }

  test("individual scoring weights can be overridden without disturbing the rest") {
    val config = JobConfig
      .fromArgs(Seq("--input", "x", "--weight-star", "0.0", "--weight-commit", "2.5"))
      .getOrElse(fail("expected a valid configuration"))

    config.weights.star shouldBe 0.0
    config.weights.commit shouldBe 2.5
    config.weights.fork shouldBe ScoreWeights.default.fork
  }

  test("a missing --input is refused with the usage text") {
    val error = JobConfig.fromArgs(Seq.empty).left.getOrElse(fail("expected a failure"))
    error should include("--input is required")
    error should include("--decoder")
  }

  test("an unresolvable decoder is refused before any work starts") {
    JobConfig.fromArgs(Seq("--input", "x", "--decoder", "com.example.Missing")).isLeft shouldBe true
  }

  test("an unreadable option value is refused with the usage text") {
    val error = JobConfig
      .fromArgs(Seq("--input", "x", "--top", "lots"))
      .left
      .getOrElse(fail("expected a failure"))
    error should include("--top")
    error should include("Options:")
  }
}
