package ghpulse.flink

import ghpulse.core.domain.GitHubEvent
import ghpulse.core.testkit.Fixtures
import org.apache.flink.streaming.api.scala._
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import scala.collection.JavaConverters._

/** Runs jobs on Flink's embedded mini cluster.
  *
  * `executeAndCollect` starts a real cluster, runs the real job graph and streams the results back,
  * so these are integration tests: windows, watermarks, state and timers all behave exactly as they
  * would on a deployed cluster. Parallelism is pinned to one so that the *order* of results is
  * reproducible too.
  */
trait FlinkSuite extends AnyFunSuite {

  protected def environment(parallelism: Int = 1): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env
  }

  /** The shared scenario as a stream, in the order the fixture lists it — which is deliberately not
    * chronological, so the watermarking is doing real work.
    */
  protected def scenarioStream(
      env: StreamExecutionEnvironment,
      maxOutOfOrderness: Duration = Duration.ofMinutes(5)
  ): DataStream[GitHubEvent] =
    EventStream.withEventTime(env.fromCollection(Fixtures.scenario), maxOutOfOrderness)

  /** Executes the job and drains every result. */
  protected def collect[T](stream: DataStream[T]): List[T] = {
    val iterator = stream.javaStream.executeAndCollect()
    try iterator.asScala.toList
    finally iterator.close()
  }
}
