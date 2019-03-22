/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class IndexWithProvidedOrderAcceptanceTest extends ExecutionEngineFunSuite
  with QueryStatisticsTestSupport with CypherComparisonSupport with AstConstructionTestSupport {

  case class TestOrder(cypherToken: String,
                       expectedOrder: Seq[Map[String, Any]] => Seq[Map[String, Any]],
                       providedOrder: Expression => ProvidedOrder)
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
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, s"MATCH (n:Awesome) WHERE n.prop2 > 1 RETURN n.prop2 ORDER BY n.prop2 $cypherToken",
        executeBefore = createSomeNodes)

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
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"""MATCH (n:Awesome) WHERE n.prop3 STARTS WITH 'foo'
           |WITH n AS nnn
           |MATCH (m)<-[r]-(nnn)
           |RETURN nnn.prop3 ORDER BY nnn.prop3 $cypherToken""".stripMargin,
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Projection")
            .withOrder(providedOrder(prop("nnn", "prop3")))
            .onTopOf(aPlan("NodeIndexSeekByRange")
              .withOrder(providedOrder(prop("n", "prop3")))
            )
        )

      result.toList should be(expectedOrder(List(
        Map("nnn.prop3" -> "fooism"), Map("nnn.prop3" -> "fooism"),
        Map("nnn.prop3" -> "footurama"), Map("nnn.prop3" -> "footurama")
      )))
    }

    test(s"$cypherToken: Order by index backed property in a plan with an Apply") {
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (a:DateString), (b:DateDate) WHERE a.ds STARTS WITH '2018' AND b.d > date(a.ds) RETURN a.ds ORDER BY a.ds $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Apply")
            .withOrder(providedOrder(prop("a", "ds")))
            .withLHS(
              aPlan("NodeIndexSeekByRange")
                .withOrder(providedOrder(prop("a", "ds")))
            )
            .withRHS(
              aPlan("NodeIndexSeekByRange")
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
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (a:Awesome)-[r]->(b) WHERE a.prop2 > 1 RETURN a.prop2, count(b) ORDER BY a.prop2 $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("EagerAggregation")
            .onTopOf(
              aPlan("Expand(All)")
                .withOrder(providedOrder(prop("a", "prop2")))
                .onTopOf(
                  aPlan("NodeIndexSeekByRange")
                    .withOrder(providedOrder(prop("a", "prop2")))))
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
          includeSomewhere.aPlan("OrderedDistinct")
            .onTopOf(
              aPlan("Expand(All)")
                .withOrder(providedOrder(prop("a", "prop2")))
                .onTopOf(
                  aPlan("NodeIndexSeekByRange")
                    .withOrder(providedOrder(prop("a", "prop2")))))
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

      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
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

      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
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

    test(s"$cypherToken: should plan partial sort after index provided order") {
      val query = s"MATCH (n:Awesome) WHERE n.prop2 > 0 RETURN n.prop2, n.prop1 ORDER BY n.prop2 $cypherToken, n.prop1 $cypherToken"
      val result = executeWith(Configs.InterpretedAndSlotted, query)

      val order = cypherToken match {
        case "ASC" => ProvidedOrder.asc(varFor("n.prop2")).asc(varFor("n.prop1"))
        case "DESC" => ProvidedOrder.desc(varFor("n.prop2")).desc(varFor("n.prop1"))
      }

      result.executionPlanDescription() should includeSomewhere
        .aPlan("PartialSort")
        .withOrder(order)
        .containingArgument("n.prop2", "n.prop1")

      result.toList should equal(expectedOrder(List(
        Map("n.prop2" -> 1, "n.prop1" -> 43),
        Map("n.prop2" -> 2, "n.prop1" -> 41),
        Map("n.prop2" -> 3, "n.prop1" -> 42),
        Map("n.prop2" -> 3, "n.prop1" -> 44),
        Map("n.prop2" -> 5, "n.prop1" -> 40),
        Map("n.prop2" -> 7, "n.prop1" -> null),
        Map("n.prop2" -> 7, "n.prop1" -> null),
        Map("n.prop2" -> 8, "n.prop1" -> null),
        Map("n.prop2" -> 9, "n.prop1" -> null)
      )))
    }

    test(s"$cypherToken: should plan partial top after index provided order") {
      val query = s"MATCH (n:Awesome) WHERE n.prop2 > 0 RETURN n.prop2, n.prop1 ORDER BY n.prop2 $cypherToken, n.prop1 $cypherToken LIMIT 4"
      val result = executeWith(Configs.InterpretedAndSlotted, query)

      val order = cypherToken match {
        case "ASC" => ProvidedOrder.asc(varFor("n.prop2")).asc(varFor("n.prop1"))
        case "DESC" => ProvidedOrder.desc(varFor("n.prop2")).desc(varFor("n.prop1"))
      }

      result.executionPlanDescription() should includeSomewhere
        .aPlan("PartialTop")
        .withOrder(order)
        .containingArgument("n.prop2", "n.prop1")

      result.toList should equal(expectedOrder(List(
        Map("n.prop2" -> 1, "n.prop1" -> 43),
        Map("n.prop2" -> 2, "n.prop1" -> 41),
        Map("n.prop2" -> 3, "n.prop1" -> 42),
        Map("n.prop2" -> 3, "n.prop1" -> 44),
        Map("n.prop2" -> 5, "n.prop1" -> 40),
        Map("n.prop2" -> 7, "n.prop1" -> null),
        Map("n.prop2" -> 7, "n.prop1" -> null),
        Map("n.prop2" -> 8, "n.prop1" -> null),
        Map("n.prop2" -> 9, "n.prop1" -> null)
      )).take(4))
    }

    test(s"$cypherToken: should plan partial top1 after index provided order") {
      val query = s"MATCH (n:Awesome) WHERE n.prop2 > 0 RETURN n.prop2, n.prop1 ORDER BY n.prop2 $cypherToken, n.prop1 $cypherToken LIMIT 1"
      val result = executeWith(Configs.InterpretedAndSlotted, query)

      val order = cypherToken match {
        case "ASC" => ProvidedOrder.asc(varFor("n.prop2")).asc(varFor("n.prop1"))
        case "DESC" => ProvidedOrder.desc(varFor("n.prop2")).desc(varFor("n.prop1"))
      }

      result.executionPlanDescription() should includeSomewhere
        .aPlan("PartialTop")
        .withOrder(order)
        .containingArgument("n.prop2", "n.prop1")

      result.toList should equal(expectedOrder(List(
        Map("n.prop2" -> 1, "n.prop1" -> 43),
        Map("n.prop2" -> 2, "n.prop1" -> 41),
        Map("n.prop2" -> 3, "n.prop1" -> 42),
        Map("n.prop2" -> 3, "n.prop1" -> 44),
        Map("n.prop2" -> 5, "n.prop1" -> 40),
        Map("n.prop2" -> 7, "n.prop1" -> null),
        Map("n.prop2" -> 7, "n.prop1" -> null),
        Map("n.prop2" -> 8, "n.prop1" -> null),
        Map("n.prop2" -> 9, "n.prop1" -> null)
      )).take(1))
    }
  }

  test("Order by index backed for composite index for ranges on two properties") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expectedAscAsc = List(
      Map("n.prop1" -> 42, "n.prop2" -> 0), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 43, "n.prop2" -> 2),
      Map("n.prop1" -> 44, "n.prop2" -> 3), Map("n.prop1" -> 45, "n.prop2" -> 2)
    )

    val expectedAscDesc = List(
      Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 0),
      Map("n.prop1" -> 43, "n.prop2" -> 2), Map("n.prop1" -> 43, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 3), Map("n.prop1" -> 45, "n.prop2" -> 2)
    )

    val expectedDescAsc = List(
      Map("n.prop1" -> 45, "n.prop2" -> 2), Map("n.prop1" -> 44, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 43, "n.prop2" -> 2),
      Map("n.prop1" -> 42, "n.prop2" -> 0), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 3)
    )

    val expectedDescDesc = List(
      Map("n.prop1" -> 45, "n.prop2" -> 2), Map("n.prop1" -> 44, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 2), Map("n.prop1" -> 43, "n.prop2" -> 1),
      Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 0)
    )

    val var1 = varFor("n.prop1")
    val var2 = varFor("n.prop2")

    val propAsc = ProvidedOrder.asc(prop("n", "prop1")).asc(prop("n", "prop2"))
    val propDesc = ProvidedOrder.desc(prop("n", "prop1")).desc(prop("n", "prop2"))
    val varAsc = ProvidedOrder.asc(var1).asc(var2)
    val varDesc = ProvidedOrder.desc(var1).desc(var2)

    Seq(
      (Configs.InterpretedAndSlottedAndMorsel, "n.prop1 ASC", expectedAscAsc, false, propAsc, varAsc, ProvidedOrder.empty),
      (Configs.InterpretedAndSlottedAndMorsel, "n.prop1 DESC", expectedDescDesc, false, propDesc, varDesc, ProvidedOrder.empty),
      (Configs.InterpretedAndSlottedAndMorsel, "n.prop1 ASC, n.prop2 ASC", expectedAscAsc, false, propAsc, varAsc, ProvidedOrder.empty),
      (Configs.InterpretedAndSlotted, "n.prop1 ASC, n.prop2 DESC", expectedAscDesc, true, propAsc, varAsc, ProvidedOrder.asc(var1).desc(var2)),
      (Configs.InterpretedAndSlotted, "n.prop1 DESC, n.prop2 ASC", expectedDescAsc, true, propDesc, varDesc, ProvidedOrder.desc(var1).asc(var2)),
      (Configs.InterpretedAndSlottedAndMorsel, "n.prop1 DESC, n.prop2 DESC", expectedDescDesc, false, propDesc, varDesc, ProvidedOrder.empty)
    ).foreach {
      case (config, orderByString, expected, shouldSort, indexOrder, projectionOrder, sortOrder) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3
             |RETURN n.prop1, n.prop2
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(config, query)

        // Then
        result.toList should equal(expected)

        result.executionPlanDescription() should includeSomewhere
          .aPlan("Projection")
            .withOrder(projectionOrder)
            .onTopOf(aPlan("Filter")
                .containingArgumentRegex(".*cached\\[n.prop2\\] <= .*".r)
                .onTopOf(aPlan("NodeIndexSeek(range,exists)")
                  .withOrder(indexOrder)
                  .containingArgument(":Label(prop1,prop2)")
                )
            )

        if (shouldSort)
          result.executionPlanDescription() should includeSomewhere
            .aPlan("PartialSort")
              .withOrder(sortOrder)
              .containingArgument("n.prop1", "n.prop2")
        else result.executionPlanDescription() should not(includeSomewhere.aPlan("PartialSort"))
    }
  }

  test("Order by not indexed backed for composite index on exists") {
    // Since exists does not give type we can't get order from the index

    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expectedAscAsc = List(
      Map("n.prop1" -> 42, "n.prop2" -> 0), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 43, "n.prop2" -> 2),
      Map("n.prop1" -> 44, "n.prop2" -> 3), Map("n.prop1" -> 45, "n.prop2" -> 2), Map("n.prop1" -> 45, "n.prop2" -> 5)
    )

    val expectedAscDesc = List(
      Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 0),
      Map("n.prop1" -> 43, "n.prop2" -> 2), Map("n.prop1" -> 43, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 3), Map("n.prop1" -> 45, "n.prop2" -> 5), Map("n.prop1" -> 45, "n.prop2" -> 2)
    )

    val expectedDescAsc = List(
      Map("n.prop1" -> 45, "n.prop2" -> 2), Map("n.prop1" -> 45, "n.prop2" -> 5), Map("n.prop1" -> 44, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 43, "n.prop2" -> 2),
      Map("n.prop1" -> 42, "n.prop2" -> 0), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 3)
    )

    val expectedDescDesc = List(
      Map("n.prop1" -> 45, "n.prop2" -> 5), Map("n.prop1" -> 45, "n.prop2" -> 2), Map("n.prop1" -> 44, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 2), Map("n.prop1" -> 43, "n.prop2" -> 1),
      Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 0)
    )

    val var1 = varFor("n.prop1")
    val var2 = varFor("n.prop2")

    Seq(
      ("n.prop1 ASC", ProvidedOrder.asc(var1), expectedAscAsc),
      ("n.prop1 DESC", ProvidedOrder.desc(var1), expectedDescAsc),
      ("n.prop1 ASC, n.prop2 ASC", ProvidedOrder.asc(var1).asc(var2), expectedAscAsc),
      ("n.prop1 ASC, n.prop2 DESC", ProvidedOrder.asc(var1).desc(var2), expectedAscDesc),
      ("n.prop1 DESC, n.prop2 ASC", ProvidedOrder.desc(var1).asc(var2), expectedDescAsc),
      ("n.prop1 DESC, n.prop2 DESC", ProvidedOrder.desc(var1).desc(var2), expectedDescDesc)
    ).foreach {
      case (orderByString, sortOrder, expected) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND exists(n.prop2)
             |RETURN n.prop1, n.prop2
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.InterpretedAndSlotted, query, executeExpectedFailures = false) // TODO morsel

        // Then
        result.executionPlanDescription() should includeSomewhere
          .aPlan("Sort")
            .withOrder(sortOrder)
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeek(range,exists)")
                .containingArgument(":Label(prop1,prop2)")
              )
            )

        result.toList should equal(expected)
    }
  }

  test("Order by partially indexed backed for composite index on part of the order by") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val propAsc = ProvidedOrder.asc(prop("n", "prop1")).asc(prop("n", "prop2"))
    val propDesc = ProvidedOrder.desc(prop("n", "prop1")).desc(prop("n", "prop2"))
    val varAsc = ProvidedOrder.asc(varFor("n.prop1")).asc(varFor("n.prop2"))
    val varDesc = ProvidedOrder.desc(varFor("n.prop1")).desc(varFor("n.prop2"))

    val map_40_5_a_true = Map("n.prop1" -> 40, "n.prop2" -> 5, "n.prop3" -> "a", "n.prop4" -> true)
    val map_40_5_b_false = Map("n.prop1" -> 40, "n.prop2" -> 5, "n.prop3" -> "b", "n.prop4" -> false)
    val map_40_5_b_true = Map("n.prop1" -> 40, "n.prop2" -> 5, "n.prop3" -> "b", "n.prop4" -> true)
    val map_40_5_c_null = Map("n.prop1" -> 40, "n.prop2" -> 5, "n.prop3" -> "c", "n.prop4" -> null)

    val map_41_2_b_null = Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> "b", "n.prop4" -> null)
    val map_41_2_d_null = Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> "d", "n.prop4" -> null)

    val map_41_4_a_true = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "a", "n.prop4" -> true)
    val map_41_4_b_false = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "b", "n.prop4" -> false)
    val map_41_4_c_false = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "c", "n.prop4" -> false)
    val map_41_4_c_true = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "c", "n.prop4" -> true)
    val map_41_4_c_null = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "c", "n.prop4" -> null)
    val map_41_4_d_null = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "d", "n.prop4" -> null)

    Seq(
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 ASC", propAsc,
        varAsc.asc(varFor("n.prop3")).asc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_40_5_a_true, map_40_5_b_false, map_40_5_b_true, map_40_5_c_null, map_41_2_b_null, map_41_2_d_null,
            map_41_4_a_true, map_41_4_b_false, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 DESC", propAsc,
        varAsc.asc(varFor("n.prop3")).desc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_40_5_a_true, map_40_5_b_true, map_40_5_b_false, map_40_5_c_null, map_41_2_b_null, map_41_2_d_null,
            map_41_4_a_true, map_41_4_b_false, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_d_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 ASC", propAsc,
        varAsc.desc(varFor("n.prop3")).asc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_40_5_c_null, map_40_5_b_false, map_40_5_b_true, map_40_5_a_true, map_41_2_d_null, map_41_2_b_null,
            map_41_4_d_null, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_b_false, map_41_4_a_true)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 DESC", propAsc,
        varAsc.desc(varFor("n.prop3")).desc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_40_5_c_null, map_40_5_b_true, map_40_5_b_false, map_40_5_a_true, map_41_2_d_null, map_41_2_b_null,
            map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_b_false, map_41_4_a_true)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 ASC", propDesc,
        varDesc.asc(varFor("n.prop3")).asc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_41_4_a_true, map_41_4_b_false, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null,
            map_41_2_b_null, map_41_2_d_null, map_40_5_a_true, map_40_5_b_false, map_40_5_b_true, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 DESC", propDesc,
        varDesc.asc(varFor("n.prop3")).desc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_41_4_a_true, map_41_4_b_false, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_d_null,
            map_41_2_b_null, map_41_2_d_null, map_40_5_a_true, map_40_5_b_true, map_40_5_b_false, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC", propDesc,
        varDesc.desc(varFor("n.prop3")).asc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_41_4_d_null, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_b_false, map_41_4_a_true,
            map_41_2_d_null, map_41_2_b_null, map_40_5_c_null, map_40_5_b_false, map_40_5_b_true, map_40_5_a_true)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC", propDesc,
        varDesc.desc(varFor("n.prop3")).desc(varFor("n.prop4")), "n.prop3, n.prop4",
        Seq(map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_b_false, map_41_4_a_true,
            map_41_2_d_null, map_41_2_b_null, map_40_5_c_null, map_40_5_b_true, map_40_5_b_false, map_40_5_a_true)),

      ("n.prop1 ASC, n.prop2 ASC, n.prop4 ASC, n.prop3 ASC", propAsc,
        varAsc.asc(varFor("n.prop4")).asc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_40_5_b_false, map_40_5_a_true, map_40_5_b_true, map_40_5_c_null, map_41_2_b_null, map_41_2_d_null,
            map_41_4_b_false, map_41_4_c_false, map_41_4_a_true, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop4 ASC, n.prop3 DESC", propAsc,
        varAsc.asc(varFor("n.prop4")).desc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_40_5_b_false, map_40_5_b_true, map_40_5_a_true, map_40_5_c_null, map_41_2_d_null, map_41_2_b_null,
            map_41_4_c_false, map_41_4_b_false, map_41_4_c_true, map_41_4_a_true, map_41_4_d_null, map_41_4_c_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop4 DESC, n.prop3 ASC", propAsc,
        varAsc.desc(varFor("n.prop4")).asc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_40_5_c_null, map_40_5_a_true, map_40_5_b_true, map_40_5_b_false, map_41_2_b_null, map_41_2_d_null,
            map_41_4_c_null, map_41_4_d_null, map_41_4_a_true, map_41_4_c_true, map_41_4_b_false, map_41_4_c_false)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop4 DESC, n.prop3 DESC", propAsc,
        varAsc.desc(varFor("n.prop4")).desc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_40_5_c_null, map_40_5_b_true, map_40_5_a_true, map_40_5_b_false, map_41_2_d_null, map_41_2_b_null,
            map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_a_true, map_41_4_c_false, map_41_4_b_false)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 ASC, n.prop3 ASC", propDesc,
        varDesc.asc(varFor("n.prop4")).asc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_41_4_b_false, map_41_4_c_false, map_41_4_a_true, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null,
            map_41_2_b_null, map_41_2_d_null, map_40_5_b_false, map_40_5_a_true, map_40_5_b_true, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 ASC, n.prop3 DESC", propDesc,
        varDesc.asc(varFor("n.prop4")).desc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_41_4_c_false, map_41_4_b_false, map_41_4_c_true, map_41_4_a_true, map_41_4_d_null, map_41_4_c_null,
            map_41_2_d_null, map_41_2_b_null, map_40_5_b_false, map_40_5_b_true, map_40_5_a_true, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 DESC, n.prop3 ASC", propDesc,
        varDesc.desc(varFor("n.prop4")).asc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_41_4_c_null, map_41_4_d_null, map_41_4_a_true, map_41_4_c_true, map_41_4_b_false, map_41_4_c_false,
            map_41_2_b_null, map_41_2_d_null, map_40_5_c_null, map_40_5_a_true, map_40_5_b_true, map_40_5_b_false)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 DESC, n.prop3 DESC", propDesc,
        varDesc.desc(varFor("n.prop4")).desc(varFor("n.prop3")), "n.prop4, n.prop3",
        Seq(map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_a_true, map_41_4_c_false, map_41_4_b_false,
            map_41_2_d_null, map_41_2_b_null, map_40_5_c_null, map_40_5_b_true, map_40_5_a_true, map_40_5_b_false))
    ).foreach {
      case (orderByString, orderIndex, sortOrder, sortItem, expected) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 < 42 AND n.prop2 > 0
             |RETURN n.prop1, n.prop2, n.prop3, n.prop4
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.InterpretedAndSlotted, query)

       // Then
        result.executionPlanDescription() should includeSomewhere
          .aPlan("PartialSort")
            .containingArgument("n.prop1, n.prop2", sortItem)
            .withOrder(sortOrder)
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Filter")
                .containingArgumentRegex(".*cached\\[n.prop2\\] > .*".r)
                .onTopOf(aPlan("NodeIndexSeek(range,exists)")
                  .withOrder(orderIndex)
                  .containingArgument(":Label(prop1,prop2)")
                )
              )
            )

        result.toComparableResult should equal(expected)
    }
  }

  // Min and Max

  for ((TestOrder(cypherToken, expectedOrder, providedOrder), functionName) <- List((ASCENDING, "min"), (DESCENDING, "max"))) {
    test(s"$cypherToken-$functionName: should use provided index order with range") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder(prop("n", "prop1"))))
            )
          )

      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40), // min
        Map(s"$functionName(n.prop1)" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order for multiple types") {
      graph.execute("CREATE (:Awesome {prop1: 'hallo'})")
      graph.execute("CREATE (:Awesome {prop1: 35.5})")

      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder(prop("n", "prop1"))))
            )
          )

      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 35.5), // min
        Map(s"$functionName(n.prop1)" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order with renamed property") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n.prop1 AS prop RETURN $functionName(prop) AS extreme", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder(prop("n", "prop1"))))
              )
            )
          )

      val expected = expectedOrder(List(
        Map("extreme" -> 40), // min
        Map("extreme" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order with renamed variable") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n as m RETURN $functionName(m.prop1) AS extreme", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder(prop("n", "prop1"))))
              )
            )
          )

      val expected = expectedOrder(List(
        Map("extreme" -> 40), // min
        Map("extreme" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order with renamed variable and property") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n as m WITH m.prop1 AS prop RETURN $functionName(prop) AS extreme", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("Projection")
                  .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder(prop("n", "prop1"))))
                )
              )
            )
          )

      val expected = expectedOrder(List(
        Map("extreme" -> 40), // min
        Map("extreme" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order for empty index"){
      graph.createIndex("B", "prop")

      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n: B) WHERE n.prop > 0 RETURN $functionName(n.prop)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop]", "n").withOrder(providedOrder(prop("n", "prop"))))
            )
          )

      val expected = List(Map(s"$functionName(n.prop)" -> null))
      result.toList should equal(expected)
    }

    test(s"$cypherToken-$functionName: should use provided index order with ORDER BY on same property") {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1) ORDER BY $functionName(n.prop1) $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n").withOrder(providedOrder(prop("n", "prop1"))))
            )
          )

      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40), // min
        Map(s"$functionName(n.prop1)" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: cannot use provided index order for prop2 when ORDER BY prop1") {
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n.prop1 AS prop1, n.prop2 as prop2 ORDER BY prop1 RETURN $functionName(prop2)",
        executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = expectedOrder(List(
        Map(s"$functionName(prop2)" -> 1), // min
        Map(s"$functionName(prop2)" -> 5) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order for nested functions with $functionName as inner") {
      graph.execute("CREATE (:Awesome {prop3: 'ha'})")

      //should give the length of the alphabetically smallest/largest prop3 string
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN size($functionName(n.prop3)) AS agg", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Projection")
          .onTopOf(aPlan("Optional")
            .onTopOf(aPlan("Limit")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop3]", "n").withOrder(providedOrder(prop("n", "prop3"))))
              )
            )
          )

      val expected = expectedOrder(List(
        Map("agg" -> 3), // min: aaa
        Map("agg" -> 2) // max: ha
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should give correct result for nested functions with $functionName as outer") {
      graph.execute("CREATE (:Awesome {prop3: 'ha'})")

      //should give the length of the shortest/longest prop3 string
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN $functionName(size(n.prop3)) AS agg", executeBefore = createSomeNodes)

      val expected = expectedOrder(List(
        Map("agg" -> 2), // min: ha
        Map("agg" -> 9) // max: footurama
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should give correct result for nested functions in several depths") {
      graph.execute("CREATE (:Awesome {prop3: 'ha'})")

      //should give the length of the shortest/longest prop3 string
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN $functionName(size(trim(replace(n.prop3, 'a', 'aa')))) AS agg",
        executeBefore = createSomeNodes)

      val expected = expectedOrder(List(
        Map("agg" -> 3), // min: haa
        Map("agg" -> 11) // max: footuraamaa
      )).head
      result.toList should equal(List(expected))
    }

    // The planer doesn't yet support composite range scans, so we can't avoid the aggregation
    test(s"$cypherToken-$functionName: cannot use provided index order from composite index") {
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 AND n.prop2 > 0 RETURN $functionName(n.prop1), $functionName(n.prop2)",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should includeSomewhere.aPlan("EagerAggregation")

      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40, s"$functionName(n.prop2)" -> 1), // min
        Map(s"$functionName(n.prop1)" -> 44, s"$functionName(n.prop2)" -> 5) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: cannot use provided index order with multiple aggregations") {
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1), count(n.prop1)", executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40, "count(n.prop1)" -> 10), // min
        Map(s"$functionName(n.prop1)" -> 44, "count(n.prop1)" -> 10) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: cannot use provided index order with grouping expression (caused by return n.prop2)") {
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1), n.prop2", executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = functionName match {
        case "min" =>
          Set(
            Map(s"$functionName(n.prop1)" -> 43, "n.prop2" -> 1),
            Map(s"$functionName(n.prop1)" -> 41, "n.prop2" -> 2),
            Map(s"$functionName(n.prop1)" -> 42, "n.prop2" -> 3),
            Map(s"$functionName(n.prop1)" -> 40, "n.prop2" -> 5)
          )
        case "max" =>
          Set(
            Map(s"$functionName(n.prop1)" -> 43, "n.prop2" -> 1),
            Map(s"$functionName(n.prop1)" -> 41, "n.prop2" -> 2),
            Map(s"$functionName(n.prop1)" -> 44, "n.prop2" -> 3),
            Map(s"$functionName(n.prop1)" -> 40, "n.prop2" -> 5)
          )
      }

      result.toSet should equal(expected)
    }

    test(s"$cypherToken-$functionName: should plan aggregation for index scan") {
      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:Awesome) RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)

      // index scan provide values but not order, since we don't know the property type
      result.executionPlanDescription() should
        includeSomewhere.aPlan("EagerAggregation")
          .onTopOf(aPlan("NodeIndexScan").withExactVariables("cached[n.prop1]", "n"))

      val expected = expectedOrder(List(
        Map(s"$functionName(n.prop1)" -> 40), // min
        Map(s"$functionName(n.prop1)" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should plan aggregation without index") {

      createLabeledNode(Map("foo" -> 2), "B")

      val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
        s"MATCH (n:B) WHERE n.foo > 0 RETURN $functionName(n.foo)", executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("NodeByLabelScan")
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = functionName match {
        case "min" => Set(Map(s"$functionName(n.foo)" -> 1))
        case "max" => Set(Map(s"$functionName(n.foo)" -> 2))
      }
      result.toSet should equal(expected)
    }
  }

  // Only tested in ASC mode because it's hard to make compatibility check out otherwise
  test("ASC: Order by index backed property in a plan with an outer join") {
    // Be careful with what is created in createSomeNodes. It underwent careful cardinality tuning to get exactly the plan we want here.
    val result = executeWith(Configs.InterpretedAndSlotted,
      """MATCH (b:B {foo:1, bar:1})
        |OPTIONAL MATCH (a:Awesome)-[r]->(b) USING JOIN ON b
        |WHERE a.prop3 > 'foo'
        |RETURN a.prop3 ORDER BY a.prop3
        |""".stripMargin,
      executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("NodeLeftOuterHashJoin")
          .withOrder(ProvidedOrder.asc(prop("a", "prop3")))
          .withRHS(
            aPlan("Expand(All)")
              .withOrder(ProvidedOrder.asc(prop("a", "prop3")))
              .onTopOf(
                aPlan("NodeIndexSeekByRange")
                  .withOrder(ProvidedOrder.asc(prop("a", "prop3")))))
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


  // Nodes for composite index independent of already existing indexes
  private def createNodesForComposite() = {
    graph.execute(
      """
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'a', prop4: true})
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'c'})
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'b', prop4: true})
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'b', prop4: false})
        |CREATE (:Label {prop1: 41, prop2: 2, prop3: 'b'})
        |CREATE (:Label {prop1: 41, prop2: 2, prop3: 'd'})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'a', prop4: true})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'c', prop4: true})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'c', prop4: false})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'c'})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'b', prop4: false})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'd'})
        |CREATE (:Label {prop1: 42, prop2: 3})
        |CREATE (:Label {prop1: 42, prop2: 1})
        |CREATE (:Label {prop1: 42, prop2: 0})
        |CREATE (:Label {prop1: 43, prop2: 1})
        |CREATE (:Label {prop1: 43, prop2: 2})
        |CREATE (:Label {prop1: 44, prop2: 3})
        |CREATE (:Label {prop1: 45, prop2: 2})
        |CREATE (:Label {prop1: 45, prop2: 5})
        |CREATE (:Label {prop1: 44, prop3: 'g'})
        |CREATE (:Label {prop1: 42, prop3: 'h'})
        |CREATE (:Label {prop1: 41})
        |CREATE (:Label {prop1: 42})
        |CREATE (:Label {prop2: 4})
        |CREATE (:Label {prop2: 2})
        |CREATE (:Label {prop2: 1, prop3: 'f'})
        |CREATE (:Label {prop2: 2, prop3: 'g'})
        |CREATE (:Label {prop3: 'h'})
        |CREATE (:Label {prop3: 'g'})
      """.stripMargin)
  }
}
