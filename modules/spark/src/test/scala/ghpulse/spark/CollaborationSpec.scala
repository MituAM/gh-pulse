package ghpulse.spark

import org.scalatest.matchers.should.Matchers

class CollaborationSpec extends SparkSuite with Matchers {

  private lazy val events = scenarioEvents().cache()

  test("a contribution is one actor in one repository, however often they acted") {
    val contributed =
      Collaboration.contributions(events).collect().map(r => r.getString(0) -> r.getString(1))

    // grace touched acme/probe three times in the scenario but contributes one row.
    contributed.count(_ == ("grace" -> "acme/probe")) shouldBe 1
    contributed should have size 7
  }

  test("finds the pair that worked together across more than one repository") {
    val pairs = Collaboration.pairs(events, minSharedRepos = 2).collect()

    pairs should have size 1
    pairs.head.getAs[String]("actor_a") shouldBe "alan"
    pairs.head.getAs[String]("actor_b") shouldBe "grace"
    pairs.head.getAs[Long]("shared_repos") shouldBe 2L
    pairs.head.getAs[Seq[String]]("repos").toList shouldBe List("acme/probe", "acme/rocket")
  }

  test("the self-join never pairs an actor with themselves or lists a pair twice") {
    val pairs = Collaboration.pairs(events, minSharedRepos = 1).collect()

    pairs.foreach { row =>
      val (a, b) = (row.getAs[String]("actor_a"), row.getAs[String]("actor_b"))
      withClue(s"($a, $b): ") {
        a should not be b
        a should be < b
      }
    }
    pairs.map(r => r.getAs[String]("actor_a") -> r.getAs[String]("actor_b")).distinct should
      have size pairs.length
  }

  test("every pair of contributors to a repository is found") {
    // acme/rocket has four contributors, so 4 choose 2 = 6 pairs; acme/probe has two, so 1 pair;
    // alan/grace is the same pair in both, leaving six distinct pairs. acme/telemetry has a single
    // contributor and therefore contributes none.
    Collaboration.pairs(events, minSharedRepos = 1).count() shouldBe 6L
  }

  test("ranks repositories by their number of distinct contributors") {
    val busiest = Collaboration.mostContributors(events, limit = 3).collect()

    busiest.map(r => r.getString(0) -> r.getLong(1)) shouldBe
      Array("acme/rocket" -> 4L, "acme/probe" -> 2L, "acme/telemetry" -> 1L)
  }
}
