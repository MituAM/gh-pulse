package ghpulse.core.analytics

import ghpulse.core.typeclass.Monoid

import scala.annotation.tailrec

/** The `k` greatest elements seen so far, held in descending order.
  *
  * Bounded at `k` after every operation, so a "top 20" can be computed without ever collecting the
  * full ranking: Spark merges partial results with `treeAggregate`, Flink merges them inside a
  * windowed process function, and neither one materialises the tail.
  *
  * The instance forms a monoid over [[merge]] provided the `Ordering` is total — that is, no two
  * distinct elements compare equal. Under a non-total ordering the result still contains `k`
  * elements of maximal rank, but *which* of a set of tied elements survives depends on the order
  * the merges happened in, and associativity is lost. Callers pick total orderings (score first,
  * then a unique name) for exactly this reason.
  */
final class TopK[A] private (val k: Int, val entries: Vector[A]) extends Serializable {

  def add(a: A)(implicit ord: Ordering[A]): TopK[A] = {
    val (greater, rest) = entries.span(ord.gt(_, a))
    if (greater.size >= k) this // already holding k elements that outrank `a`
    else new TopK(k, (greater :+ a) ++ rest.take(k - greater.size - 1))
  }

  def merge(other: TopK[A])(implicit ord: Ordering[A]): TopK[A] =
    new TopK(k.max(other.k), TopK.mergeDescending(entries, other.entries, k.max(other.k)))

  def toList: List[A] = entries.toList

  def isEmpty: Boolean = entries.isEmpty

  def size: Int = entries.size

  override def toString: String = entries.mkString("TopK(", ", ", ")")

  override def equals(other: Any): Boolean = other match {
    case that: TopK[_] => k == that.k && entries == that.entries
    case _ => false
  }

  override def hashCode(): Int = (k, entries).hashCode()
}

object TopK extends Serializable {

  def empty[A](k: Int): TopK[A] = {
    require(k > 0, s"k must be positive, got $k")
    new TopK[A](k, Vector.empty)
  }

  def of[A: Ordering](k: Int, elements: A*): TopK[A] =
    elements.foldLeft(empty[A](k))(_ add _)

  def from[A: Ordering](k: Int, elements: TraversableOnce[A]): TopK[A] =
    elements.foldLeft(empty[A](k))(_ add _)

  /** Not `implicit`: the instance depends on the runtime value `k`, so it is summoned explicitly at
    * each call site rather than resolved from the type alone.
    */
  def monoid[A: Ordering](k: Int): Monoid[TopK[A]] =
    Monoid.instance(empty[A](k))(_ merge _)

  /** Linear merge of two descending vectors, stopping as soon as `k` elements are collected. */
  private def mergeDescending[A](xs: Vector[A], ys: Vector[A], k: Int)(implicit
      ord: Ordering[A]
  ): Vector[A] = {
    @tailrec
    def loop(a: Vector[A], b: Vector[A], acc: Vector[A]): Vector[A] =
      if (acc.size == k) acc
      else if (a.isEmpty) acc ++ b.take(k - acc.size)
      else if (b.isEmpty) acc ++ a.take(k - acc.size)
      else if (ord.gteq(a.head, b.head)) loop(a.tail, b, acc :+ a.head)
      else loop(a, b.tail, acc :+ b.head)

    loop(xs, ys, Vector.empty)
  }
}
