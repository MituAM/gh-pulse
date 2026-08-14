package ghpulse.spark

import ghpulse.core.domain.GitHubEvent
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** One local Spark session shared by every suite in this module.
  *
  * Starting a session costs a few seconds, so it is created once per suite and configured for a
  * small input: four shuffle partitions instead of the default two hundred, which otherwise
  * dominates the runtime of a test over a few thousand rows.
  */
trait SparkSuite extends AnyFunSuite with BeforeAndAfterAll {

  protected lazy val spark: SparkSession = {
    val session = SparkPipeline.session("gh-pulse-tests", master = Some("local[2]"))
    session.conf.set("spark.sql.shuffle.partitions", "4")
    session.sparkContext.setLogLevel("WARN")
    session
  }

  /** The shared scenario as a dataset, deliberately spread over several partitions. */
  protected def scenarioEvents(partitions: Int = 3): Dataset[GitHubEvent] = {
    import spark.implicits._
    spark.createDataset(ghpulse.core.testkit.Fixtures.scenario).repartition(partitions)
  }

  override protected def afterAll(): Unit =
    try spark.close()
    finally super.afterAll()
}
