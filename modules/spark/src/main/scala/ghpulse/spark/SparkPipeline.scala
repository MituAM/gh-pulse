package ghpulse.spark

import ghpulse.core.analytics.ActorBurst
import ghpulse.core.cli.JobConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Batch entry point: reads a corpus of events once and runs every analysis over it.
  *
  * ```
  * sbt demoSpark
  * spark-submit --class ghpulse.spark.SparkPipeline gh-pulse-spark-assembly.jar \
  *   --input data/raw/'*.json.gz' --top 25
  * ```
  */
object SparkPipeline {

  def main(args: Array[String]): Unit =
    JobConfig.fromArgs(args.toSeq) match {
      case Left(error) =>
        Console.err.println(error)
        sys.exit(2)
      case Right(config) =>
        val spark = session("gh-pulse-batch")
        try run(spark, config)
        finally spark.close()
    }

  def session(appName: String, master: Option[String] = None): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)
      // Without this Spark resolves the driver through the machine's hostname, which fails on
      // many laptops and CI runners.
      .config("spark.driver.host", "localhost")
      // Every timestamp in this project is UTC epoch millis; pinning the session keeps SQL-side
      // formatting from drifting with the machine's locale.
      .config("spark.sql.session.timeZone", "UTC")

    // Never override a master that spark-submit already chose; only supply one when running
    // standalone, as the demo and the tests do.
    val submitted = new SparkConf().contains("spark.master")
    val resolved = master.orElse(if (submitted) None else Some("local[*]"))
    resolved.fold(builder)(builder.master).getOrCreate()
  }

  def run(spark: SparkSession, config: JobConfig): Unit = {
    val (events, report) = EventSource.readAndReport(spark, config.input, config.decoder)

    // Plain ASCII: this lands in terminals and CI logs with every imaginable encoding.
    println(s"""
       |gh-pulse (batch)
       |  input     ${config.input}
       |  decoder   ${config.decoderName}
       |  events    ${report.accepted} accepted, ${report.rejected} rejected
       |""".stripMargin)

    val leaderboard = RepoLeaderboard.usingDataset(events, config.topK, config.weights)
    println(s"Top ${config.topK} repositories\n${RepoLeaderboard.render(leaderboard)}\n")

    val timeline = ActivityTimeline.compute(events, config.windowMillis, config.weights)
    println("Busiest repositories per window")
    ActivityTimeline.leadersPerWindow(timeline, perWindow = 3).show(30, truncate = false)

    val bursts = BurstReport.compute(events, config.burst).collect().sortBy(_.endedAt)
    println(
      s"Actor bursts (${config.burst.minEvents}+ events in " +
        s"${config.burst.windowMillis / 60000} minutes)\n${BurstReport.render(bursts)}\n"
    )

    println("Repositories with the most distinct contributors")
    Collaboration.mostContributors(events, limit = 10).show(10, truncate = false)

    val collaborators = Collaboration.pairs(events)
    println("Repeat collaborators (people sharing more than one repository)")
    if (collaborators.isEmpty) println("  none in this input\n")
    else collaborators.show(20, truncate = false)

    config.output.foreach(path => write(path, timeline, bursts, collaborators))
  }

  private def write(
      path: String,
      timeline: DataFrame,
      bursts: Array[ActorBurst],
      collaborators: DataFrame
  ): Unit = {
    val spark = timeline.sparkSession
    import spark.implicits._

    timeline.write.mode("overwrite").json(s"$path/timeline")
    bursts.toSeq.toDF().write.mode("overwrite").json(s"$path/bursts")
    collaborators.write.mode("overwrite").json(s"$path/collaborators")
    println(s"Results written under $path")
  }
}
