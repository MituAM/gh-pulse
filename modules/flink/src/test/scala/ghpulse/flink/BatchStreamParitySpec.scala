package ghpulse.flink

import ghpulse.core.analytics.{ActivityStats, ActorBurst, BurstConfig, ScoreWeights}
import ghpulse.core.codec.GitHubArchiveDecoder
import ghpulse.core.domain.GitHubEvent
import ghpulse.core.testkit.Fixtures.Minute
import ghpulse.core.testkit.{Fixtures, Reference}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.scalatest.matchers.should.Matchers

import java.time.Duration

/** The claim this project is built around: the same rules give the same answers in batch and in
  * streaming.
  *
  * The two engines are never compared to each other directly — putting Spark and Flink on one
  * classpath is a dependency fight with nothing to gain. They are each pinned to
  * [[ghpulse.core.testkit.Reference]], which works the answers out in plain Scala:
  *
  *   - batch side — `RepoLeaderboardSpec`, `ActivityTimelineSpec` and `BurstReportSpec` in the
  *     `spark` module assert `batch == Reference`;
  *   - streaming side — this suite and its neighbours assert `streaming == Reference`.
  *
  * Equality with a common third value is what makes the two equal to each other, and it says which
  * one is at fault when they are not.
  *
  * Unlike the other suites, this one runs over the committed sample of a real GH Archive hour and
  * reads it through the real file source, so decoding, watermarking, windowing and keyed state are
  * all exercised on data that nobody wrote to make a test pass.
  */
class BatchStreamParitySpec extends FlinkSuite with Matchers {

  private val weights = ScoreWeights.default
  private val windowSize = 10 * Minute
  private lazy val sample: List[GitHubEvent] = Fixtures.sampleEvents

  private def sampleStream(env: StreamExecutionEnvironment): DataStream[GitHubEvent] =
    EventStream.fromPath(
      env,
      Fixtures.sampleFile.toString,
      GitHubArchiveDecoder,
      Duration.ofMinutes(5)
    )

  test("the sample is worth testing against: a full hour, many repositories, out of order") {
    sample should have size 1335
    sample.map(_.repo.name).distinct.size should be > 500
    sample.map(_.createdAt).distinct.size should be > 100

    val arrival = sample.map(_.createdAt)
    withClue("the sample must contain out-of-order records or watermarks prove nothing: ") {
      arrival should not be arrival.sorted
    }
  }

  test("windowed activity over a real hour matches the reference") {
    val env = environment()
    val windows = collect(WindowedActivityStream.tumbling(sampleStream(env), windowSize, weights))

    windows should contain theSameElementsAs Reference.windows(windowSize, weights, sample)
  }

  test("no event is lost or double counted between the file and the windows") {
    val env = environment()
    val windows = collect(WindowedActivityStream.tumbling(sampleStream(env), windowSize, weights))

    windows.map(_.stats.events).sum shouldBe sample.size.toLong
    windows.map(_.stats.commits).sum shouldBe sample.map(ActivityStats.of).map(_.commits).sum
  }

  test("the streaming leaderboard matches the reference ranking") {
    val env = environment()
    val activity = WindowedActivityStream.tumbling(sampleStream(env), windowSize, weights)
    val ranked = collect(TrendingRepos.rank(activity, k = 5))
      .groupBy(_.windowEnd)
      .map { case (end, rows) => end -> rows.sortBy(_.rank).map(_.repo) }

    ranked shouldBe Reference.trending(windowSize, 5, weights, sample).map { case (end, top) =>
      end -> top.map(_.repo)
    }
  }

  test("burst detection over a real hour matches the reference") {
    val env = environment()
    val config = BurstConfig(minEvents = 4, windowMillis = windowSize, cooldownMillis = 20 * Minute)
    val bursts = collect(PushBursts(sampleStream(env), config)).sorted(ActorBurst.ordering)

    bursts should not be empty
    bursts shouldBe Reference.bursts(config, sample)
  }
}
