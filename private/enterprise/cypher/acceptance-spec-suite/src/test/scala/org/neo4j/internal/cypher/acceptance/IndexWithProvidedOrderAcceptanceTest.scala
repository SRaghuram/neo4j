/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.ir.v4_0.ProvidedOrder
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class IndexWithProvidedOrderAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  case class TestOrder(cypherToken: String,
                       expectedOrder: Seq[Map[String, Any]] => Seq[Map[String, Any]],
                       providedOrder: String => ProvidedOrder)
  val ASCENDING = TestOrder("ASC", x => x, ProvidedOrder.asc)
  val DESCENDING = TestOrder("DESC", x => x.reverse, ProvidedOrder.desc)

  override def beforeEach(): Unit = {
    super.beforeEach()
    createSomeNodes()
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Awesome", "prop1", "prop2")
    graph.createIndex("Awesome", "prop3")
    graph.createIndex("Awesome", "prop4")
    graph.createIndex("DateString", "ds")
    graph.createIndex("DateDate", "d")
  }

  // Invoked once before the Tx and once in the same Tx
  def createSomeNodes(): Unit = {
    graph.execute(
      """CREATE (:Awesome {prop1: 40, prop2: 5})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 41, prop2: 2})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 42, prop2: 3})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 43, prop2: 1})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 44, prop2: 3})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 7})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 9})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 8})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 7})-[:R]->(:B)
        |CREATE (:Awesome {prop3: 'footurama', prop4:'bar'})-[:R]->(:B {foo:1, bar:1})
        |CREATE (:Awesome {prop3: 'fooism', prop4:'rab'})-[:R]->(:B {foo:1, bar:1})
        |CREATE (:Awesome {prop3: 'aismfama', prop4:'rab'})-[:R]->(:B {foo:1, bar:1})
        |
        |FOREACH (i in range(0, 10000) | CREATE (:Awesome {prop3: 'aaa'})-[:R]->(:B) )
        |
        |CREATE (:DateString {ds: '2018-01-01'})
        |CREATE (:DateString {ds: '2018-02-01'})
        |CREATE (:DateString {ds: '2018-04-01'})
        |CREATE (:DateString {ds: '2017-03-01'})
        |
        |CREATE (:DateDate {d: date('2018-02-10')})
        |CREATE (:DateDate {d: date('2018-01-10')})
      """.stripMargin)
  }

  for (TestOrder(cypherToken, expectedOrder, providedOrder) <- List(ASCENDING, DESCENDING)) {

    test(s"$cypherToken: should use index order for range predicate when returning that property") {
      val result = executeWith(Configs.InterpretedAndSlotted, s"MATCH (n:Awesome) WHERE n.prop2 > 1 RETURN n.prop2 ORDER BY n.prop2 $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should not (includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop2" -> 2), Map("n.prop2" -> 2),
        Map("n.prop2" -> 3), Map("n.prop2" -> 3),
        Map("n.prop2" -> 3), Map("n.prop2" -> 3),
        Map("n.prop2" -> 5), Map("n.prop2" -> 5),
        Map("n.prop2" -> 7), Map("n.prop2" -> 7),
        Map("n.prop2" -> 7), Map("n.prop2" -> 7),
        Map("n.prop2" -> 8), Map("n.prop2" -> 8),
        Map("n.prop2" -> 9), Map("n.prop2" -> 9)
      )))
    }

    test(s"$cypherToken: Order by index backed property renamed in an earlier WITH") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"""MATCH (n:Awesome) WHERE n.prop3 STARTS WITH 'foo'
           |WITH n AS nnn
           |MATCH (m)<-[r]-(nnn)
           |RETURN nnn.prop3 ORDER BY nnn.prop3 $cypherToken""".stripMargin, executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Projection")
            .withOrder(providedOrder("nnn.prop3"))
            .onTopOf(aPlan("NodeIndexSeekByRange")
              .withOrder(providedOrder("n.prop3"))
            )
        )

      result.toList should be(expectedOrder(List(
        Map("nnn.prop3" -> "fooism"), Map("nnn.prop3" -> "fooism"),
        Map("nnn.prop3" -> "footurama"), Map("nnn.prop3" -> "footurama")
      )))
    }

    test(s"$cypherToken: Order by index backed property in a plan with an Apply") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (a:DateString), (b:DateDate) WHERE a.ds STARTS WITH '2018' AND b.d > date(a.ds) RETURN a.ds ORDER BY a.ds $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Apply")
            .withOrder(providedOrder("a.ds"))
            .withLHS(
              aPlan("NodeIndexSeekByRange")
                .withOrder(providedOrder("a.ds"))
            )
            .withRHS(
              aPlan("NodeIndexSeekByRange")
                .withOrder(providedOrder("b.d"))
            )
        )

      result.toList should be(expectedOrder(List(
        Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
        Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
        Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
        Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
        Map("a.ds" -> "2018-02-01"), Map("a.ds" -> "2018-02-01"),
        Map("a.ds" -> "2018-02-01"), Map("a.ds" -> "2018-02-01")
      )))
    }

    test(s"$cypherToken: Order by index backed property in a plan with an aggregation and an expand") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (a:Awesome)-[r]->(b) WHERE a.prop2 > 1 RETURN a.prop2, count(b) ORDER BY a.prop2 $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("EagerAggregation")
            .onTopOf(
              aPlan("Expand(All)")
                .withOrder(providedOrder("a.prop2"))
                .onTopOf(
                  aPlan("NodeIndexSeekByRange")
                    .withOrder(providedOrder("a.prop2"))))
        )

      result.toList should be(expectedOrder(List(
        Map("a.prop2" -> 2, "count(b)" -> 2),
        Map("a.prop2" -> 3, "count(b)" -> 4),
        Map("a.prop2" -> 5, "count(b)" -> 2),
        Map("a.prop2" -> 7, "count(b)" -> 4),
        Map("a.prop2" -> 8, "count(b)" -> 2),
        Map("a.prop2" -> 9, "count(b)" -> 2)
      )))
    }

    test(s"$cypherToken: Order by index backed property in a plan with a distinct") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (a:Awesome)-[r]->(b) WHERE a.prop2 > 1 RETURN DISTINCT a.prop2 ORDER BY a.prop2 $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Distinct")
            .onTopOf(
              aPlan("Expand(All)")
                .withOrder(providedOrder("a.prop2"))
                .onTopOf(
                  aPlan("NodeIndexSeekByRange")
                    .withOrder(providedOrder("a.prop2"))))
        )

      result.toList should be(expectedOrder(List(
        Map("a.prop2" -> 2),
        Map("a.prop2" -> 3),
        Map("a.prop2" -> 5),
        Map("a.prop2" -> 7),
        Map("a.prop2" -> 8),
        Map("a.prop2" -> 9)
      )))
    }

    // This is supported because internally all kernel indexes which support ordering will just scan and filter to serve contains
    test(s"$cypherToken: Order by index backed property should plan with provided order (contains scan)") {
      createStringyNodes()

      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop3 CONTAINS 'cat' RETURN n.prop3 ORDER BY n.prop3 $cypherToken",
        executeBefore = createStringyNodes)

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop3" -> "bobcat"), Map("n.prop3" -> "bobcat"),
        Map("n.prop3" -> "catastrophy"), Map("n.prop3" -> "catastrophy"),
        Map("n.prop3" -> "poodlecatilicious"), Map("n.prop3" -> "poodlecatilicious"),
        Map("n.prop3" -> "scat"), Map("n.prop3" -> "scat"),
        Map("n.prop3" -> "tree-cat-bog"), Map("n.prop3" -> "tree-cat-bog"),
        Map("n.prop3" -> "whinecathog"), Map("n.prop3" -> "whinecathog")
      )))
    }

    // This is supported because internally all kernel indexes which support ordering will just scan and filter to serve ends with
    test(s"$cypherToken: Order by index backed property should plan with provided order (ends with scan)") {
      createStringyNodes()

      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop3 ENDS WITH 'og' RETURN n.prop3 ORDER BY n.prop3 $cypherToken",
        executeBefore = createStringyNodes)

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop3" -> "dog"), Map("n.prop3" -> "dog"),
        Map("n.prop3" -> "flog"), Map("n.prop3" -> "flog"),
        Map("n.prop3" -> "tree-cat-bog"), Map("n.prop3" -> "tree-cat-bog"),
        Map("n.prop3" -> "whinecathog"), Map("n.prop3" -> "whinecathog")
      )))
    }
  }

  // Min and Max

  for ((TestOrder(cypherToken, expectedOrder, providedOrder), functionName) <- List((ASCENDING, "min"), (DESCENDING, "max"))) {
    test(s"$cypherToken-$functionName: should not plan aggregation") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Limit")
          .onTopOf(aPlan("Projection")
            .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder("n.prop1")))
          )

      //expectedOrder.head corresponds to limit 1
      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40), // min
        Map(s"$functionName(n.prop1)" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should not plan aggregation for multiple types") {
      graph.execute("CREATE (:Awesome {prop1: 'hallo'})")

      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Limit")
          .onTopOf(aPlan("Projection")
            .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder("n.prop1"))))

      //expectedOrder.head corresponds to limit 1
      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40), // min
        Map(s"$functionName(n.prop1)" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should not plan aggregation with rename") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n.prop1 AS prop RETURN $functionName(prop) AS extreme", executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("Limit")
      plan should includeSomewhere.aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder("n.prop1"))
      plan shouldNot includeSomewhere.aPlan("Sort")
      plan shouldNot includeSomewhere.aPlan("Aggregation")

      //expectedOrder.head corresponds to limit 1
      val expected = expectedOrder(List(
        Map("extreme" -> 40), // min
        Map("extreme" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should not plan aggregation for nested functions with $functionName as inner") {
      graph.execute("CREATE (:Awesome {prop3: 'ha'})")

      //should give the length of the alphabetically smallest/largest prop3 string
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN size($functionName(n.prop3)) AS agg", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Projection")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop3]", "n").withOrder(providedOrder("n.prop3")))
            )
          )

      //expectedOrder.head corresponds to limit 1
      val expected = expectedOrder(List(
        Map("agg" -> 3), // min: aaa
        Map("agg" -> 2) // max: ha
      )).head
      result.toList should equal(List(expected))
    }

    // There is no index for size so have to use aggregation, will still add it as an interesting order
    test(s"$cypherToken-$functionName: should give correct result for nested functions with $functionName as outer") {
      graph.execute("CREATE (:Awesome {prop3: 'ha'})")

      //should give the length of the shortest/longest prop3 string
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN $functionName(size(n.prop3)) AS agg", executeBefore = createSomeNodes)

      //expectedOrder.head corresponds to limit 1
      val expected = expectedOrder(List(
        Map("agg" -> 2), // min: ha
        Map("agg" -> 9) // max: footurama
      )).head
      result.toList should equal(List(expected))
    }

    // There is no index for the inner functions so have to use aggregation, will still add it as an interesting order
    test(s"$cypherToken-$functionName: should give correct result for nested functions in several depths") {
      graph.execute("CREATE (:Awesome {prop3: 'ha'})")

      //should give the length of the shortest/longest prop3 string
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN $functionName(size(trim(replace(n.prop3, 'a', 'aa')))) AS agg", executeBefore = createSomeNodes)

      //expectedOrder.head corresponds to limit 1
      val expected = expectedOrder(List(
        Map("agg" -> 3), // min: haa
        Map("agg" -> 11) // max: footuraamaa
      )).head
      result.toList should equal(List(expected))
    }

    // The planer doesn't yet support composite range scans, so we can't avoid the aggregation
    test(s"$cypherToken-$functionName: should give correct result for multiple aggregations") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 AND n.prop2 > 0 RETURN $functionName(n.prop1), $functionName(n.prop2)", executeBefore = createSomeNodes)

      //expectedOrder.head corresponds to limit 1
      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40, s"$functionName(n.prop2)" -> 1), // min
        Map(s"$functionName(n.prop1)" -> 44, s"$functionName(n.prop2)" -> 5) // max
      )).head
      result.toList should equal(List(expected))
    }

  }

  // Only tested in ASC mode because it's hard to make compatibility check out otherwise
  test("ASC: Order by index backed property in a plan with an outer join") {
    // Be careful with what is created in createSomeNodes. It underwent careful cardinality tuning to get exactly the plan we want here.
    val result =  executeWith(Configs.InterpretedAndSlotted,
      """MATCH (b:B {foo:1, bar:1})
        |OPTIONAL MATCH (a:Awesome)-[r]->(b) USING JOIN ON b
        |WHERE a.prop3 > 'foo'
        |RETURN a.prop3 ORDER BY a.prop3
        |""".stripMargin,
      executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("NodeLeftOuterHashJoin")
          .withOrder(ProvidedOrder.asc("a.prop3"))
          .withRHS(
            aPlan("Expand(All)")
              .withOrder(ProvidedOrder.asc("a.prop3"))
              .onTopOf(
                aPlan("NodeIndexSeekByRange")
                  .withOrder(ProvidedOrder.asc("a.prop3"))))
      )

    result.toList should be(List(
      Map("a.prop3" -> "fooism"), Map("a.prop3" -> "fooism"),
      Map("a.prop3" -> "footurama"), Map("a.prop3" -> "footurama"),
      Map("a.prop3" -> null), Map("a.prop3" -> null)
    ))
  }

  // Some nodes which are suitable for CONTAINS and ENDS WITH testing
  private def createStringyNodes() =
    graph.execute(
      """CREATE (:Awesome {prop3: 'scat'})
        |CREATE (:Awesome {prop3: 'bobcat'})
        |CREATE (:Awesome {prop3: 'poodlecatilicious'})
        |CREATE (:Awesome {prop3: 'dog'})
        |CREATE (:Awesome {prop3: 'flog'})
        |CREATE (:Awesome {prop3: 'catastrophy'})
        |CREATE (:Awesome {prop3: 'whinecathog'})
        |CREATE (:Awesome {prop3: 'scratch'})
        |CREATE (:Awesome {prop3: 'tree-cat-bog'})
        |""".stripMargin)

}
