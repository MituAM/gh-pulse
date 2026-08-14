package ghpulse.core.codec

import ghpulse.core.domain.GitHubEvent

import scala.util.control.NonFatal

/** Turns one raw record into a domain event, or explains why it could not.
  *
  * This is the seam for running the pipelines on data that is not GH Archive. Both engines take an
  * `EventDecoder` rather than parsing inline, so pointing the jobs at a different corpus means
  * writing one implementation of this trait and naming it with `--decoder`; nothing else changes.
  *
  * Implementations must be `Serializable` — the instance is captured by closures that Spark and
  * Flink ship to their workers — and must not throw: a malformed record in a firehose is normal and
  * is reported as a `Left`, not as a failed job.
  */
trait EventDecoder extends Serializable {
  def decode(record: String): Either[String, GitHubEvent]
}

object EventDecoder {

  /** Decoders that can be selected by short name on the command line. */
  val builtIn: Map[String, EventDecoder] = Map("gharchive" -> GitHubArchiveDecoder)

  /** Turns a decoder that throws into one that reports.
    *
    * The trait asks implementations not to throw, but a `--decoder` can point at anybody's code and
    * an exception escaping into a Spark task or a Flink operator takes the whole job down. Both
    * engines wrap the configured decoder in this before using it, so a bad record costs one row
    * rather than the run.
    */
  def guarded(decoder: EventDecoder): EventDecoder = new EventDecoder {
    def decode(record: String): Either[String, GitHubEvent] =
      try decoder.decode(record)
      catch { case NonFatal(e) => Left(s"${e.getClass.getSimpleName}: ${e.getMessage}") }
  }

  /** Resolves `--decoder`: either a built-in short name, or the fully qualified name of a user
    * supplied `EventDecoder` (a Scala `object` or a class with a no-argument constructor) that is
    * on the job's classpath.
    */
  def resolve(name: String): Either[String, EventDecoder] =
    builtIn.get(name).map(Right(_)).getOrElse(loadByClassName(name))

  private def loadByClassName(className: String): Either[String, EventDecoder] = {
    val loader = Thread.currentThread().getContextClassLoader
    // A Scala `object Foo` compiles to class `Foo$` holding the singleton in a `MODULE$` field;
    // try that first so users can supply either an object or a class.
    val instance =
      try Right(Class.forName(className + "$", true, loader).getField("MODULE$").get(null))
      catch {
        case _: ClassNotFoundException | _: NoSuchFieldException =>
          try Right(Class.forName(className, true, loader).getDeclaredConstructor().newInstance())
          catch { case e: ReflectiveOperationException => Left(e.toString) }
      }

    instance
      .flatMap {
        case decoder: EventDecoder => Right(decoder)
        case other => Left(s"$className is a ${other.getClass.getName}, not an EventDecoder")
      }
      .left
      .map(reason =>
        s"could not load decoder '$className' ($reason); " +
          s"built-in names are ${builtIn.keys.mkString(", ")}"
      )
  }
}
