package ghpulse.core.cli

import scala.util.Try

/** A minimal `--key value` parser.
  *
  * Deliberately dependency-free: both fat jars are submitted to clusters whose classpaths are
  * already crowded, and an argument parser is not worth a shaded library.
  */
final case class Args(values: Map[String, String]) {

  def get(key: String): Option[String] = values.get(key)

  def str(key: String, default: String): String = values.getOrElse(key, default)

  def int(key: String, default: Int): Int = parsed(key, default, _.toInt)

  def long(key: String, default: Long): Long = parsed(key, default, _.toLong)

  def double(key: String, default: Double): Double = parsed(key, default, _.toDouble)

  def bool(key: String, default: Boolean): Boolean = parsed(key, default, _.toBoolean)

  private def parsed[A](key: String, default: A, f: String => A): A =
    values.get(key) match {
      case None => default
      case Some(raw) =>
        Try(f(raw)).getOrElse(
          throw new IllegalArgumentException(
            s"--$key: cannot read '$raw' as ${default.getClass.getSimpleName}"
          )
        )
    }
}

object Args {

  /** Reads `--key value` pairs. A `--key` immediately followed by another `--key` is treated as a
    * boolean flag set to true, so `--verbose` works without an explicit value.
    */
  def parse(argv: Seq[String]): Args = {
    val pairs = argv.foldLeft((Map.empty[String, String], Option.empty[String])) {
      case ((acc, pending), token) if token.startsWith("--") =>
        val key = token.drop(2)
        (pending.fold(acc)(p => acc + (p -> "true")), Some(key))
      case ((acc, Some(key)), value) =>
        (acc + (key -> value), None)
      case ((acc, None), _) =>
        (acc, None) // positional arguments are not used by any job
    }
    Args(pairs._2.fold(pairs._1)(p => pairs._1 + (p -> "true")))
  }
}
