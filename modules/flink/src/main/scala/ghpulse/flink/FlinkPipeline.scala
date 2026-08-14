package ghpulse.flink

import ghpulse.core.analytics.{ActorBurst, TimeBucket, WindowedActivity}
import ghpulse.core.cli.JobConfig
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._

import java.time.Duration

/** Streaming entry point: replays the same corpus the batch job reads, as a stream.
  *
  * ```
  * sbt demoFlink
  * flink run -c ghpulse.flink.FlinkPipeline gh-pulse-flink-assembly.jar \
  *   --input /data/events.jsonl --top 25
  * ```
  */
object FlinkPipeline {

  def main(args: Array[String]): Unit =
    JobConfig.fromArgs(args.toSeq) match {
      case Left(error) =>
        Console.err.println(error)
        sys.exit(2)
      case Right(config) =>
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        build(env, config)
        env.execute("gh-pulse-streaming")
    }

  def build(env: StreamExecutionEnvironment, config: JobConfig): Unit = {
    // A file sink only finalises its part files at a checkpoint, so writing output implies
    // checkpointing. Printing does not, which keeps the default demo free of state directories.
    config.output.foreach(_ => env.enableCheckpointing(5000))

    val events = EventStream.fromPath(
      env,
      config.input,
      config.decoder,
      Duration.ofMillis(config.maxOutOfOrdernessMillis)
    )

    val slide = (config.windowMillis / 6).max(1000L)

    emit(
      WindowedActivityStream.tumbling(events, config.windowMillis, config.weights),
      config,
      "activity",
      (a: WindowedActivity) =>
        f"${TimeBucket.formatMinute(a.windowStart)} ${a.repo}%-50s score=${a.score}%7.1f " +
          f"events=${a.stats.events}%4d commits=${a.stats.commits}%4d"
    )

    emit(
      TrendingRepos(events, config.windowMillis, slide, config.topK, config.weights),
      config,
      "trending",
      (r: RankedRepo) =>
        f"${TimeBucket.formatMinute(r.windowStart)}..${TimeBucket.formatMinute(r.windowEnd)} " +
          f"#${r.rank}%-3d ${r.repo}%-50s ${r.score}%7.1f"
    )

    emit(
      PushBursts(events, config.burst),
      config,
      "bursts",
      (b: ActorBurst) =>
        f"${b.actor}%-25s ${b.events}%3d events  " +
          f"${TimeBucket.formatMinute(b.startedAt)} -> ${TimeBucket.formatMinute(b.endedAt)}"
    )
  }

  /** Prints by default; writes to a rolling file sink when `--output` names a directory. */
  private def emit[T](
      stream: DataStream[T],
      config: JobConfig,
      name: String,
      show: T => String
  ): Unit = {
    val rendered = stream.map((record: T) => show(record)).name(s"render/$name")

    config.output match {
      case None =>
        rendered.print(name)
      case Some(base) =>
        rendered
          .sinkTo(
            FileSink
              .forRowFormat(new Path(s"$base/$name"), new SimpleStringEncoder[String]("UTF-8"))
              .build()
          )
          .name(name)
    }
  }
}
