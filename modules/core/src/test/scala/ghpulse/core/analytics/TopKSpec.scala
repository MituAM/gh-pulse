package ghpulse.core.analytics

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TopKSpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {

  test("keeps the greatest k elements in descending order") {
    TopK.of(3, 5, 1, 9, 3, 7).toList shouldBe List(9, 7, 5)
  }

  test("never grows beyond k, however the elements arrive") {
    forAll(Gen.listOf(Gen.chooseNum(-100, 100)), Gen.chooseNum(1, 10)) { (values, k) =>
      TopK.from(k, values).size shouldBe values.size.min(k)
    }
  }

  test("agrees with sorting the whole input") {
    forAll(Gen.listOf(Gen.chooseNum(-1000, 1000)).map(_.distinct), Gen.chooseNum(1, 12)) {
      (values, k) =>
        TopK.from(k, values).toList shouldBe values.sorted.reverse.take(k)
    }
  }

  test("merging partial results equals ranking the whole input at once") {
    forAll(
      Gen.listOf(Gen.chooseNum(-1000, 1000)).map(_.distinct),
      Gen.chooseNum(1, 8),
      Gen.chooseNum(1, 5)
    ) { (values, k, partitions) =>
      val merged = values
        .grouped((values.size / partitions).max(1))
        .map(TopK.from(k, _))
        .foldLeft(TopK.empty[Int](k))(_ merge _)

      merged.toList shouldBe TopK.from(k, values).toList
    }
  }

  test("an element that cannot make the cut is discarded without disturbing the rest") {
    val full = TopK.of(2, 10, 8)
    full.add(1).toList shouldBe List(10, 8)
    full.add(9).toList shouldBe List(10, 9)
  }

  test("ranks repositories by score and breaks ties on name") {
    import RepoActivity.ordering
    val tied = List(
      RepoActivity("z/one", ActivityStats.empty, 10.0),
      RepoActivity("a/two", ActivityStats.empty, 10.0),
      RepoActivity("m/three", ActivityStats.empty, 25.0)
    )
    TopK.from(2, tied).toList.map(_.repo) shouldBe List("m/three", "z/one")
  }

  test("k must be positive") {
    an[IllegalArgumentException] should be thrownBy TopK.empty[Int](0)
  }
}
