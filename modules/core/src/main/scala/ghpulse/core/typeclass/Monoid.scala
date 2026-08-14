package ghpulse.core.typeclass

/** A type that can be combined associatively and has an identity element.
  *
  * Hand-rolled rather than taken from cats so that `core` imposes no library version on the Spark
  * and Flink classpaths, which are both hostile to extra transitive dependencies.
  *
  * Instances must satisfy, for all `x`, `y`, `z`:
  *   - associativity: `combine(combine(x, y), z) == combine(x, combine(y, z))`
  *   - identity: `combine(x, empty) == x == combine(empty, x)`
  *
  * Those two laws are the reason the same aggregation can run under Spark's `reduceByKey` (which
  * re-associates freely across partitions) and Flink's incremental window aggregation (which folds
  * one record at a time) and still produce identical results. `MonoidLawsSpec` checks them.
  */
trait Monoid[A] extends Serializable {
  def empty: A
  def combine(x: A, y: A): A

  def combineAll(xs: TraversableOnce[A]): A = xs.foldLeft(empty)(combine)
}

// `Serializable` on the companions throughout `core` is not decoration: Spark and Flink serialise
// whatever a closure reaches, and a closure that mentions `Monoid[ActivityStats]` reaches this
// module. Declaring it keeps that from becoming a runtime failure on the first shuffle.
object Monoid extends Serializable {
  def apply[A](implicit instance: Monoid[A]): Monoid[A] = instance

  def instance[A](zero: A)(f: (A, A) => A): Monoid[A] = new Monoid[A] {
    val empty: A = zero
    def combine(x: A, y: A): A = f(x, y)
  }

  implicit final class MonoidOps[A](private val self: A) extends AnyVal {
    def |+|(other: A)(implicit M: Monoid[A]): A = M.combine(self, other)
  }
}
