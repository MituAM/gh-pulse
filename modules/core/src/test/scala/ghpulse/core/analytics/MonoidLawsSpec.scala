package ghpulse.core.analytics

import ghpulse.core.typeclass.Monoid
import ghpulse.core.typeclass.Monoid.MonoidOps
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** The laws that make distributed aggregation safe.
  *
  * Spark re-associates a `reduceByKey` however the partitioning happens to fall, and Flink folds
  * incrementally as records arrive. Both are only correct if the combine is associative and `empty`
  * really is an identity, so those two properties are checked here rather than assumed — every
  * aggregate in the project is built from an instance covered by this suite.
  */
class MonoidLawsSpec extends AnyFunSuite with ScalaCheckPropertyChecks {

  private val counts: Gen[Long] = Gen.chooseNum(0L, 1000L)

  private implicit val arbStats: Arbitrary[ActivityStats] = Arbitrary(
    for {
      events <- counts
      pushes <- counts
      commits <- counts
      prs <- counts
      merged <- counts
      issues <- counts
      comments <- counts
      stars <- counts
      forks <- counts
      added <- counts
      deleted <- counts
    } yield ActivityStats(
      events,
      pushes,
      commits,
      prs,
      merged,
      issues,
      comments,
      stars,
      forks,
      added,
      deleted
    )
  )

  // TopK is only associative under a *total* order: two elements that compare equal but are not
  // equal would let the merge order decide the winner. Deriving the stats from the score makes
  // (score, repo) uniquely identify an element, which is the condition TopK documents.
  private implicit val arbRepoActivity: Arbitrary[RepoActivity] = Arbitrary(
    for {
      name <- Gen.identifier
      score <- Gen.chooseNum(0.0, 1000.0)
    } yield RepoActivity(name, ActivityStats.empty.copy(events = score.toLong), score)
  )

  private def checkLaws[A](name: String)(implicit arb: Arbitrary[A], M: Monoid[A]): Unit = {
    test(s"$name: combine is associative") {
      forAll { (x: A, y: A, z: A) =>
        assert(((x |+| y) |+| z) == (x |+| (y |+| z)))
      }
    }

    test(s"$name: empty is a two-sided identity") {
      forAll { (x: A) =>
        assert((x |+| M.empty) == x)
        assert((M.empty |+| x) == x)
      }
    }
  }

  checkLaws[ActivityStats]("ActivityStats")

  {
    // TopK's monoid is parameterised by k, so it is summoned rather than resolved implicitly.
    implicit val topKMonoid: Monoid[TopK[RepoActivity]] = TopK.monoid[RepoActivity](5)
    implicit val arbTopK: Arbitrary[TopK[RepoActivity]] = Arbitrary(
      Gen.listOf(arbRepoActivity.arbitrary).map(TopK.from(5, _))
    )
    checkLaws[TopK[RepoActivity]]("TopK(5)")
  }

  test("aggregating events one at a time equals aggregating them in any grouping") {
    forAll(Gen.listOf(arbStats.arbitrary), Gen.chooseNum(1, 8)) { (stats, chunkSize) =>
      val sequential = Monoid[ActivityStats].combineAll(stats)
      val chunked = Monoid[ActivityStats].combineAll(
        stats.grouped(chunkSize).map(Monoid[ActivityStats].combineAll(_))
      )
      assert(sequential == chunked)
    }
  }
}
