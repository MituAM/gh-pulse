package ghpulse.core.testkit

import ghpulse.core.domain._

import java.io.File

/** Hand-built events shared by the core, Spark and Flink suites.
  *
  * The Spark and Flink modules depend on this one `test->test`, so all three engines are exercised
  * against byte-identical input. That is the only reason it is meaningful to assert that batch and
  * streaming agree.
  */
object Fixtures {

  /** 2024-01-15T15:00:00Z — the hour the committed sample was taken from. */
  val T0: Long = 1705330800000L

  val Minute: Long = 60000L
  val Hour: Long = 60 * Minute

  def push(
      id: String,
      at: Long,
      actor: String,
      repo: String,
      commits: Int = 1
  ): GitHubEvent =
    event(
      id,
      "PushEvent",
      at,
      actor,
      repo,
      Payload.empty.copy(
        ref = Some("refs/heads/main"),
        pushSize = Some(commits),
        distinctSize = Some(commits),
        commits = (1 to commits).toList.map(n =>
          PushCommit(f"$id%s-$n%02d", actor, s"$actor@example.com", s"commit $n", distinct = true)
        )
      )
    )

  def pullRequest(
      id: String,
      at: Long,
      actor: String,
      repo: String,
      action: String = "opened",
      merged: Boolean = false,
      additions: Int = 0,
      deletions: Int = 0
  ): GitHubEvent =
    event(
      id,
      "PullRequestEvent",
      at,
      actor,
      repo,
      Payload.empty.copy(
        action = Some(action),
        pullRequestNumber = Some(1),
        merged = Some(merged),
        additions = Some(additions),
        deletions = Some(deletions),
        changedFiles = Some(1)
      )
    )

  def star(id: String, at: Long, actor: String, repo: String): GitHubEvent =
    event(id, "WatchEvent", at, actor, repo, Payload.empty.copy(action = Some("started")))

  def fork(id: String, at: Long, actor: String, repo: String): GitHubEvent =
    event(id, "ForkEvent", at, actor, repo, Payload.empty)

  def issue(id: String, at: Long, actor: String, repo: String): GitHubEvent =
    event(id, "IssuesEvent", at, actor, repo, Payload.empty.copy(action = Some("opened")))

  def comment(id: String, at: Long, actor: String, repo: String): GitHubEvent =
    event(id, "IssueCommentEvent", at, actor, repo, Payload.empty.copy(action = Some("created")))

  def event(
      id: String,
      eventType: String,
      at: Long,
      actor: String,
      repo: String,
      payload: Payload = Payload.empty
  ): GitHubEvent =
    GitHubEvent(
      id = id,
      eventType = eventType,
      createdAt = at,
      actor = Actor(actor.hashCode.toLong.abs, actor),
      repo = Repo(repo.hashCode.toLong.abs, repo),
      org = Some(repo.takeWhile(_ != '/')),
      payload = payload
    )

  /** A two-hour scenario with three deliberate features: a clear leader (`acme/rocket`), an actor
    * whose events are dense enough to trip the burst detector (`bot`), and events that arrive out
    * of chronological order so that streaming watermarks actually have something to reorder.
    */
  val scenario: List[GitHubEvent] = {
    val rocket = List(
      push("r1", T0 + 1 * Minute, "ada", "acme/rocket", commits = 3),
      push("r2", T0 + 4 * Minute, "ada", "acme/rocket", commits = 2),
      push("r3", T0 + 9 * Minute, "linus", "acme/rocket", commits = 5),
      pullRequest("r4", T0 + 12 * Minute, "linus", "acme/rocket", "opened"),
      pullRequest(
        "r5",
        T0 + 20 * Minute,
        "ada",
        "acme/rocket",
        "closed",
        merged = true,
        additions = 120,
        deletions = 40
      ),
      star("r6", T0 + 25 * Minute, "grace", "acme/rocket"),
      star("r7", T0 + 26 * Minute, "alan", "acme/rocket"),
      fork("r8", T0 + 70 * Minute, "grace", "acme/rocket"),
      push("r9", T0 + 95 * Minute, "ada", "acme/rocket", commits = 1)
    )

    val probe = List(
      push("p1", T0 + 3 * Minute, "grace", "acme/probe", commits = 1),
      issue("p2", T0 + 30 * Minute, "alan", "acme/probe"),
      comment("p3", T0 + 31 * Minute, "grace", "acme/probe"),
      // Deliberately out of order: this arrives after p3 but happened before it.
      push("p4", T0 + 29 * Minute, "grace", "acme/probe", commits = 2),
      push("p5", T0 + 80 * Minute, "alan", "acme/probe", commits = 1)
    )

    // Twelve events inside four minutes: over the default threshold of ten in five.
    val botStorm = (0 until 12).toList.map { n =>
      push(f"b$n%02d", T0 + 40 * Minute + n * 20000L, "bot", "acme/telemetry", commits = 1)
    }

    rocket ::: probe ::: botStorm
  }

  /** Events sorted by event time, which is what a batch job effectively sees. */
  val scenarioInEventTimeOrder: List[GitHubEvent] = scenario.sortBy(_.createdAt)

  /** The committed sample of a real GH Archive hour.
    *
    * Located by walking up from the working directory rather than by a relative path, so suites
    * behave the same under `sbt test`, `sbt spark/test` and an IDE runner.
    */
  lazy val sampleFile: File = new File(repoRoot, "data/sample/events.jsonl")

  /** Every event in the committed sample, decoded once and reused. */
  lazy val sampleEvents: List[GitHubEvent] = {
    val source = scala.io.Source.fromFile(sampleFile, "UTF-8")
    try source.getLines().flatMap(ghpulse.core.codec.GitHubArchiveDecoder.decode(_).toOption).toList
    finally source.close()
  }

  lazy val repoRoot: File = {
    def search(dir: File): File =
      if (dir == null) sys.error("could not locate the repository root (no build.sbt above cwd)")
      else if (new File(dir, "build.sbt").isFile) dir
      else search(dir.getParentFile)

    search(new File(sys.props("user.dir")).getAbsoluteFile)
  }
}
