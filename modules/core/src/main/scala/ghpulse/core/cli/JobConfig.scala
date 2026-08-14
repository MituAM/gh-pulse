package ghpulse.core.cli

import ghpulse.core.analytics.{BurstConfig, ScoreWeights}
import ghpulse.core.codec.EventDecoder

/** Everything both pipelines need to be told, parsed once.
  *
  * Sharing the configuration between the batch and the streaming entry point is what makes it
  * meaningful to run them over the same input and compare the answers: neither can quietly disagree
  * about the scoring weights or the burst threshold.
  */
final case class JobConfig(
    input: String,
    output: Option[String],
    decoder: EventDecoder,
    decoderName: String,
    topK: Int,
    weights: ScoreWeights,
    burst: BurstConfig,
    windowMillis: Long,
    maxOutOfOrdernessMillis: Long
)

object JobConfig {

  val usage: String =
    """Options:
      |  --input <path>            file, directory or glob of newline-delimited JSON  (required)
      |  --output <path>           write results here instead of printing them
      |  --decoder <name|class>    'gharchive', or your own EventDecoder's class name (default: gharchive)
      |  --top <n>                 leaderboard size                                   (default: 20)
      |  --window <minutes>        activity window width                              (default: 60)
      |  --out-of-orderness <sec>  how late a streaming event may arrive              (default: 300)
      |  --burst-events <n>        events by one actor that make a burst              (default: 10)
      |  --burst-window <minutes>  the interval those events must fall in             (default: 5)
      |  --burst-cooldown <min>    quiet period after reporting a burst               (default: 15)
      |  --weight-<kind> <double>  override a scoring weight; <kind> is one of
      |                            commit, pr, merged-pr, issue, comment, star, fork
      |""".stripMargin

  def fromArgs(argv: Seq[String]): Either[String, JobConfig] = {
    val args = Args.parse(argv)
    for {
      input <- args.get("input").toRight(s"--input is required.\n\n$usage")
      decoderName = args.str("decoder", "gharchive")
      decoder <- EventDecoder.resolve(decoderName)
      config <- read(args, input, decoderName, decoder)
    } yield config
  }

  private def read(
      args: Args,
      input: String,
      decoderName: String,
      decoder: EventDecoder
  ): Either[String, JobConfig] =
    try {
      val defaults = ScoreWeights.default
      Right(
        JobConfig(
          input = input,
          output = args.get("output"),
          decoder = decoder,
          decoderName = decoderName,
          topK = args.int("top", 20),
          weights = ScoreWeights(
            commit = args.double("weight-commit", defaults.commit),
            pullRequest = args.double("weight-pr", defaults.pullRequest),
            mergedPullRequest = args.double("weight-merged-pr", defaults.mergedPullRequest),
            issue = args.double("weight-issue", defaults.issue),
            comment = args.double("weight-comment", defaults.comment),
            star = args.double("weight-star", defaults.star),
            fork = args.double("weight-fork", defaults.fork)
          ),
          burst = BurstConfig(
            minEvents = args.int("burst-events", BurstConfig.default.minEvents),
            windowMillis = minutes(args.long("burst-window", 5)),
            cooldownMillis = minutes(args.long("burst-cooldown", 15))
          ),
          windowMillis = minutes(args.long("window", 60)),
          maxOutOfOrdernessMillis = args.long("out-of-orderness", 300) * 1000L
        )
      )
    } catch {
      case e: IllegalArgumentException => Left(s"${e.getMessage}\n\n$usage")
    }

  private def minutes(n: Long): Long = n * 60000L
}
