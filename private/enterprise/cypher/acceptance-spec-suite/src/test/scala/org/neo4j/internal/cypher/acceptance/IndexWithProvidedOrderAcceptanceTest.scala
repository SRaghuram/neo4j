/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian
import org.neo4j.values.storable.Values.pointValue

//noinspection RegExpRedundantEscape
class IndexWithProvidedOrderAcceptanceTest extends ExecutionEngineFunSuite
                                           with QueryStatisticsTestSupport with CypherComparisonSupport with AstConstructionTestSupport {

  case class TestOrder(cypherToken: String,
                       expectedOrder: Seq[Map[String, Any]] => Seq[Map[String, Any]],
                       providedOrder: Expression => ProvidedOrder)
  private val ASCENDING = TestOrder("ASC", x => x, ProvidedOrder.asc)
  private val DESCENDING = TestOrder("DESC", x => x.reverse, ProvidedOrder.desc)

  override def beforeEach(): Unit = {
    super.beforeEach()
    graph.withTx(tx => createSomeNodes(tx))
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Awesome", "prop1", "prop2")
    graph.createIndex("Awesome", "prop2", "prop2Copy")
    graph.createIndex("Awesome", "prop3")
    graph.createIndex("Awesome", "prop4")
    graph.createIndex("DateString", "ds")
    graph.createIndex("DateDate", "d")
  }

  // Invoked once before the Tx and once in the same Tx
  def createSomeNodes(tx: InternalTransaction): Unit = {
    tx.execute(
      """CREATE (:Awesome {prop1: 40, prop2: 5, prop2Copy: 5})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 41, prop2: 2, prop2Copy: 2})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 42, prop2: 3, prop2Copy: 3})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 43, prop2: 1, prop2Copy: 1})-[:R]->(:B)
        |CREATE (:Awesome {prop1: 44, prop2: 3, prop2Copy: 3})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 7, prop2Copy: 7})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 9, prop2Copy: 9})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 8, prop2Copy: 8})-[:R]->(:B)
        |CREATE (:Awesome {prop2: 7, prop2Copy: 7})-[:R]->(:B)
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
        |
        |CREATE (:Awesome {prop2: point({x:-500000, y:-500000}), prop2Copy: point({x:-500000, y:-500000})})
        |CREATE (:Awesome {prop2: point({x:-500000, y:500000}), prop2Copy: point({x:-500000, y:500000})})
        |CREATE (:Awesome {prop2: point({x:500000, y:-500000}), prop2Copy: point({x:500000, y:-500000})})
        |CREATE (:Awesome {prop2: point({x:500000, y:500000}),  prop2Copy: point({x:500000, y:500000})})
      """.stripMargin)
  }

  for (TestOrder(cypherToken, expectedOrder, providedOrder) <- List(ASCENDING, DESCENDING)) {

    test(s"$cypherToken: should use index order for exists predicate when returning that property") {
      val result = executeWith(Configs.CachedProperty, s"MATCH (n:Awesome) WHERE exists(n.prop2) RETURN n.prop2 ORDER BY n.prop2 $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should not (includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)),
        Map("n.prop2" -> 1), Map("n.prop2" -> 1),
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

    test(s"$cypherToken: should use index order for IS NOT NULL predicate when returning that property") {
      val result = executeWith(Configs.CachedProperty, s"MATCH (n:Awesome) WHERE n.prop2 IS NOT NULL RETURN n.prop2 ORDER BY n.prop2 $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should not (includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)),
        Map("n.prop2" -> 1), Map("n.prop2" -> 1),
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

    test(s"$cypherToken: should use index order for exists predicate when not returning that property") {
      val result = executeWith(Configs.CachedProperty, s"MATCH (n:Awesome) WHERE exists(n.prop2) RETURN n.prop2Copy ORDER BY n.prop2 $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should not (includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop2Copy" -> pointValue(Cartesian, -500000, -500000)), Map("n.prop2Copy" -> pointValue(Cartesian, -500000, -500000)),
        Map("n.prop2Copy" -> pointValue(Cartesian, -500000, 500000)), Map("n.prop2Copy" -> pointValue(Cartesian, -500000, 500000)),
        Map("n.prop2Copy" -> pointValue(Cartesian, 500000, -500000)), Map("n.prop2Copy" -> pointValue(Cartesian, 500000, -500000)),
        Map("n.prop2Copy" -> pointValue(Cartesian, 500000, 500000)), Map("n.prop2Copy" -> pointValue(Cartesian, 500000, 500000)),
        Map("n.prop2Copy" -> 1), Map("n.prop2Copy" -> 1),
        Map("n.prop2Copy" -> 2), Map("n.prop2Copy" -> 2),
        Map("n.prop2Copy" -> 3), Map("n.prop2Copy" -> 3),
        Map("n.prop2Copy" -> 3), Map("n.prop2Copy" -> 3),
        Map("n.prop2Copy" -> 5), Map("n.prop2Copy" -> 5),
        Map("n.prop2Copy" -> 7), Map("n.prop2Copy" -> 7),
        Map("n.prop2Copy" -> 7), Map("n.prop2Copy" -> 7),
        Map("n.prop2Copy" -> 8), Map("n.prop2Copy" -> 8),
        Map("n.prop2Copy" -> 9), Map("n.prop2Copy" -> 9)
      )))
    }

    test(s"$cypherToken: should use index order for IS NOT NULL predicate when not returning that property") {
      val result = executeWith(Configs.CachedProperty, s"MATCH (n:Awesome) WHERE n.prop2 IS NOT NULL RETURN n.prop2Copy ORDER BY n.prop2 $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should not (includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop2Copy" -> pointValue(Cartesian, -500000, -500000)), Map("n.prop2Copy" -> pointValue(Cartesian, -500000, -500000)),
        Map("n.prop2Copy" -> pointValue(Cartesian, -500000, 500000)), Map("n.prop2Copy" -> pointValue(Cartesian, -500000, 500000)),
        Map("n.prop2Copy" -> pointValue(Cartesian, 500000, -500000)), Map("n.prop2Copy" -> pointValue(Cartesian, 500000, -500000)),
        Map("n.prop2Copy" -> pointValue(Cartesian, 500000, 500000)), Map("n.prop2Copy" -> pointValue(Cartesian, 500000, 500000)),
        Map("n.prop2Copy" -> 1), Map("n.prop2Copy" -> 1),
        Map("n.prop2Copy" -> 2), Map("n.prop2Copy" -> 2),
        Map("n.prop2Copy" -> 3), Map("n.prop2Copy" -> 3),
        Map("n.prop2Copy" -> 3), Map("n.prop2Copy" -> 3),
        Map("n.prop2Copy" -> 5), Map("n.prop2Copy" -> 5),
        Map("n.prop2Copy" -> 7), Map("n.prop2Copy" -> 7),
        Map("n.prop2Copy" -> 7), Map("n.prop2Copy" -> 7),
        Map("n.prop2Copy" -> 8), Map("n.prop2Copy" -> 8),
        Map("n.prop2Copy" -> 9), Map("n.prop2Copy" -> 9)
      )))
    }

    test(s"$cypherToken: should use index for exists predicate on composite index ") {
      val result = executeWith(Configs.CachedProperty, s"MATCH (n:Awesome) WHERE exists(n.prop2) AND exists(n.prop2Copy) RETURN n.prop2 ORDER BY n.prop2 $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)),
        Map("n.prop2" -> 1), Map("n.prop2" -> 1),
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

    test(s"$cypherToken: should use index for IS NOT NULL predicate on composite index ") {
      val result = executeWith(Configs.CachedProperty, s"MATCH (n:Awesome) WHERE n.prop2 IS NOT NULL AND n.prop2Copy IS NOT NULL RETURN n.prop2 ORDER BY n.prop2 $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, -500000, 500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, -500000)),
        Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)), Map("n.prop2" -> pointValue(Cartesian, 500000, 500000)),
        Map("n.prop2" -> 1), Map("n.prop2" -> 1),
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

    test(s"$cypherToken: should use index order for range predicate when returning that property") {
      val result = executeWith(Configs.CachedProperty, s"MATCH (n:Awesome) WHERE n.prop2 > 1 RETURN n.prop2 ORDER BY n.prop2 $cypherToken",
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

    test(s"$cypherToken: Label scan") {
      val result = executeWith(Configs.All,
        s"MATCH (n:DateString) RETURN n.ds ORDER BY n $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))

      result.toList should be(expectedOrder(List(
        Map("n.ds" -> "2018-01-01"),
        Map("n.ds" -> "2018-02-01"),
        Map("n.ds" -> "2018-04-01"),
        Map("n.ds" -> "2017-03-01"),
        Map("n.ds" -> "2018-01-01"),
        Map("n.ds" -> "2018-02-01"),
        Map("n.ds" -> "2018-04-01"),
        Map("n.ds" -> "2017-03-01"),
      )))
    }

    test(s"$cypherToken: Order by index backed property renamed in an earlier WITH") {
      val result = executeWith(Configs.CachedProperty,
        s"""MATCH (n:Awesome) WHERE n.prop3 STARTS WITH 'foo'
           |WITH n AS nnn
           |MATCH (m)<-[r]-(nnn)
           |RETURN nnn.prop3 ORDER BY nnn.prop3 $cypherToken""".stripMargin,
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Projection")
            .withOrder(providedOrder(prop("nnn", "prop3")).fromLeft)
            .onTopOf(aPlan("NodeIndexSeekByRange")
              .withOrder(providedOrder(prop("n", "prop3")))
            )
        )

      result.toList should be(expectedOrder(List(
        Map("nnn.prop3" -> "fooism"), Map("nnn.prop3" -> "fooism"),
        Map("nnn.prop3" -> "footurama"), Map("nnn.prop3" -> "footurama")
      )))
    }

    test(s"$cypherToken: Order by index backed property renamed in an earlier WITH (named index)") {
      graph.withTx(tx => createNodesForNamedIndex(tx))
      graph.createIndexWithName("my_index", "Label", "prop")

      val result = executeWith(Configs.CachedProperty,
        s"""MATCH (n:Label) WHERE n.prop > 42
           |WITH n AS nnn
           |MATCH (m)<-[r]-(nnn)
           |RETURN nnn.prop ORDER BY nnn.prop $cypherToken""".stripMargin,
        executeBefore = createNodesForNamedIndex)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Projection")
            .withOrder(providedOrder(prop("nnn", "prop")).fromLeft)
            .onTopOf(aPlan("NodeIndexSeekByRange")
              .withOrder(providedOrder(prop("n", "prop")))
            )
        )

      result.toList should be(expectedOrder(List(
        Map("nnn.prop" -> 43), Map("nnn.prop" -> 43),
        Map("nnn.prop" -> 44), Map("nnn.prop" -> 44)
      )))
    }

    test(s"$cypherToken: Order by index backed property in a plan with an Apply") {
      val result = executeWith(Configs.UDF,
        s"MATCH (a:DateString), (b:DateDate) WHERE a.ds STARTS WITH '2018' AND b.d > date(a.ds) RETURN a.ds ORDER BY a.ds $cypherToken",
        executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("Apply")
            .withOrder(providedOrder(prop("a", "ds")).fromLeft)
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
      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
        s"MATCH (a:Awesome)-[r]->(b) WHERE a.prop2 > 1 RETURN a.prop2, count(b) ORDER BY a.prop2 $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("OrderedAggregation")
            .onTopOf(
              aPlan("Expand(All)")
                .withOrder(providedOrder(prop("a", "prop2")).fromLeft)
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
      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
        s"MATCH (a:Awesome)-[r]->(b) WHERE a.prop2 > 1 RETURN DISTINCT a.prop2 ORDER BY a.prop2 $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should (
        not(includeSomewhere.aPlan("Sort")) and
          includeSomewhere.aPlan("OrderedDistinct")
            .onTopOf(
              aPlan("Expand(All)")
                .withOrder(providedOrder(prop("a", "prop2")).fromLeft)
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
      graph.withTx( tx => createStringyNodes(tx))

      val result = executeWith(Configs.CachedProperty,
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
      graph.withTx(tx => createStringyNodes(tx))

      val result = executeWith(Configs.NodeIndexEndsWithScan,
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
      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

      val order = cypherToken match {
        case "ASC" => ProvidedOrder.asc(varFor("n.prop2")).asc(varFor("n.prop1")).fromLeft
        case "DESC" => ProvidedOrder.desc(varFor("n.prop2")).desc(varFor("n.prop1")).fromLeft
      }

      result.executionPlanDescription() should includeSomewhere
        .aPlan("PartialSort")
        .withOrder(order)
        .containingArgument(s"n.prop2 $cypherToken, n.prop1 $cypherToken")

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
      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

      val order = cypherToken match {
        case "ASC" => ProvidedOrder.asc(varFor("n.prop2")).asc(varFor("n.prop1")).fromLeft
        case "DESC" => ProvidedOrder.desc(varFor("n.prop2")).desc(varFor("n.prop1")).fromLeft
      }

      result.executionPlanDescription() should includeSomewhere
        .aPlan("PartialTop")
        .withOrder(order)
        .containingArgument(s"n.prop2 $cypherToken, n.prop1 $cypherToken")

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
      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

      val order = cypherToken match {
        case "ASC" => ProvidedOrder.asc(varFor("n.prop2")).asc(varFor("n.prop1")).fromLeft
        case "DESC" => ProvidedOrder.desc(varFor("n.prop2")).desc(varFor("n.prop1")).fromLeft
      }

      result.executionPlanDescription() should includeSomewhere
        .aPlan("PartialTop")
        .withOrder(order)
        .containingArgument(s"n.prop2 $cypherToken, n.prop1 $cypherToken")

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
      ("n.prop1 ASC", expectedAscAsc, false, propAsc, varAsc.fromLeft, ProvidedOrder.empty),
      ("n.prop1 DESC", expectedDescDesc, false, propDesc, varDesc.fromLeft, ProvidedOrder.empty),
      ("n.prop1 ASC, n.prop2 ASC", expectedAscAsc, false, propAsc, varAsc.fromLeft, ProvidedOrder.empty),
      ("n.prop1 ASC, n.prop2 DESC", expectedAscDesc, true, propAsc, varAsc.fromLeft, ProvidedOrder.asc(var1).desc(var2).fromLeft),
      ("n.prop1 DESC, n.prop2 ASC", expectedDescAsc, true, propDesc, varDesc.fromLeft, ProvidedOrder.desc(var1).asc(var2).fromLeft),
      ("n.prop1 DESC, n.prop2 DESC", expectedDescDesc, false, propDesc, varDesc.fromLeft, ProvidedOrder.empty)
    ).foreach {
      case (orderByString, expected, shouldSort, indexOrder, projectionOrder, sortOrder) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3
             |RETURN n.prop1, n.prop2
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.CachedProperty, query)

        // Then
        result.toList should equal(expected)

        result.executionPlanDescription() should includeSomewhere
          .aPlan("Projection")
          .withOrder(projectionOrder)
          .onTopOf(aPlan("Filter")
            .containingArgumentRegex(".*cache\\[n\\.prop2\\] <= .*".r)
            .onTopOf(aPlan("NodeIndexSeek")
              .withOrder(indexOrder)
              .containingArgumentRegex("n:Label\\(prop1, prop2\\) WHERE prop1 >= .* AND exists\\(prop2\\), cache\\[n.prop1\\], cache\\[n.prop2\\]".r)
            )
          )

        if (shouldSort)
          result.executionPlanDescription() should includeSomewhere
            .aPlan("PartialSort")
            .withOrder(sortOrder)
            .containingArgument(orderByString)
        else result.executionPlanDescription() should not(includeSomewhere.aPlan("PartialSort"))
    }
  }

  test("should handle exists and ORDER BY on two columns of composite index") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expectedResult = List(
      Map("n.prop1" -> 42, "n.prop2" -> 0), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 43, "n.prop2" -> 2),
      Map("n.prop1" -> 44, "n.prop2" -> 3), Map("n.prop1" -> 45, "n.prop2" -> 2), Map("n.prop1" -> 45, "n.prop2" -> 5)
    )
    Seq(
      ("n.prop1 ASC, n.prop2 ASC",   expectedResult.sortBy(row => ( row("n.prop1"),  row("n.prop2")))),
      ("n.prop1 ASC, n.prop2 DESC",  expectedResult.sortBy(row => ( row("n.prop1"), -row("n.prop2")))),
      ("n.prop1 DESC, n.prop2 ASC",  expectedResult.sortBy(row => (-row("n.prop1"),  row("n.prop2")))),
      ("n.prop1 DESC, n.prop2 DESC", expectedResult.sortBy(row => (-row("n.prop1"), -row("n.prop2"))))
    ).foreach {
      case (orderByString,  expected) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND exists(n.prop2)
             |RETURN n.prop1, n.prop2
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.CachedProperty, query)

        // Then
        withClue(orderByString) {
          result.executionPlanDescription() should (includeSomewhere
            .aPlan("NodeIndexSeek")
            .containingArgumentRegex("n:Label\\(prop1, prop2\\) WHERE prop1 >= .* AND exists\\(prop2\\), cache\\[n.prop1\\], cache\\[n.prop2\\]".r)
            and not(includeSomewhere.aPlan("Sort")))

          result.toList should equal(expected)
        }
    }
  }

  test("should handle exists and ORDER BY on one column of composite index") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expectedResult = List(
      Map("n.prop1" -> 42, "n.prop2" -> 0), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 43, "n.prop2" -> 2),
      Map("n.prop1" -> 44, "n.prop2" -> 3), Map("n.prop1" -> 45, "n.prop2" -> 2), Map("n.prop1" -> 45, "n.prop2" -> 5)
    )
    Seq(
      ("n.prop1 ASC",  expectedResult.map(_("n.prop1")).sorted),
      ("n.prop1 DESC", expectedResult.map(_("n.prop1")).sorted.reverse)
    ).foreach {
      case (orderByString,  expected) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND exists(n.prop2)
             |RETURN n.prop1, n.prop2
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.CachedProperty, query)

        // Then
        withClue(orderByString) {
          result.executionPlanDescription() should (includeSomewhere
            .aPlan("NodeIndexSeek")
            .containingArgumentRegex("n:Label\\(prop1, prop2\\) WHERE prop1 >= .* AND exists\\(prop2\\), cache\\[n.prop1\\], cache\\[n.prop2\\]".r)
            and not(includeSomewhere.aPlan("Sort")))


          result.toList should contain theSameElementsAs expectedResult
          result.toList.map(_("n.prop1")) should equal(expected)
        }
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
        varAsc.asc(varFor("n.prop3")).asc(varFor("n.prop4")).fromLeft,
        Seq(map_40_5_a_true, map_40_5_b_false, map_40_5_b_true, map_40_5_c_null, map_41_2_b_null, map_41_2_d_null,
          map_41_4_a_true, map_41_4_b_false, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 DESC", propAsc,
        varAsc.asc(varFor("n.prop3")).desc(varFor("n.prop4")).fromLeft,
        Seq(map_40_5_a_true, map_40_5_b_true, map_40_5_b_false, map_40_5_c_null, map_41_2_b_null, map_41_2_d_null,
          map_41_4_a_true, map_41_4_b_false, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_d_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 ASC", propAsc,
        varAsc.desc(varFor("n.prop3")).asc(varFor("n.prop4")).fromLeft,
        Seq(map_40_5_c_null, map_40_5_b_false, map_40_5_b_true, map_40_5_a_true, map_41_2_d_null, map_41_2_b_null,
          map_41_4_d_null, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_b_false, map_41_4_a_true)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 DESC", propAsc,
        varAsc.desc(varFor("n.prop3")).desc(varFor("n.prop4")).fromLeft,
        Seq(map_40_5_c_null, map_40_5_b_true, map_40_5_b_false, map_40_5_a_true, map_41_2_d_null, map_41_2_b_null,
          map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_b_false, map_41_4_a_true)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 ASC", propDesc,
        varDesc.asc(varFor("n.prop3")).asc(varFor("n.prop4")).fromLeft,
        Seq(map_41_4_a_true, map_41_4_b_false, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null,
          map_41_2_b_null, map_41_2_d_null, map_40_5_a_true, map_40_5_b_false, map_40_5_b_true, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 DESC", propDesc,
        varDesc.asc(varFor("n.prop3")).desc(varFor("n.prop4")).fromLeft,
        Seq(map_41_4_a_true, map_41_4_b_false, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_d_null,
          map_41_2_b_null, map_41_2_d_null, map_40_5_a_true, map_40_5_b_true, map_40_5_b_false, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC", propDesc,
        varDesc.desc(varFor("n.prop3")).asc(varFor("n.prop4")).fromLeft,
        Seq(map_41_4_d_null, map_41_4_c_false, map_41_4_c_true, map_41_4_c_null, map_41_4_b_false, map_41_4_a_true,
          map_41_2_d_null, map_41_2_b_null, map_40_5_c_null, map_40_5_b_false, map_40_5_b_true, map_40_5_a_true)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC", propDesc,
        varDesc.desc(varFor("n.prop3")).desc(varFor("n.prop4")).fromLeft,
        Seq(map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_c_false, map_41_4_b_false, map_41_4_a_true,
          map_41_2_d_null, map_41_2_b_null, map_40_5_c_null, map_40_5_b_true, map_40_5_b_false, map_40_5_a_true)),

      ("n.prop1 ASC, n.prop2 ASC, n.prop4 ASC, n.prop3 ASC", propAsc,
        varAsc.asc(varFor("n.prop4")).asc(varFor("n.prop3")).fromLeft,
        Seq(map_40_5_b_false, map_40_5_a_true, map_40_5_b_true, map_40_5_c_null, map_41_2_b_null, map_41_2_d_null,
          map_41_4_b_false, map_41_4_c_false, map_41_4_a_true, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop4 ASC, n.prop3 DESC", propAsc,
        varAsc.asc(varFor("n.prop4")).desc(varFor("n.prop3")).fromLeft,
        Seq(map_40_5_b_false, map_40_5_b_true, map_40_5_a_true, map_40_5_c_null, map_41_2_d_null, map_41_2_b_null,
          map_41_4_c_false, map_41_4_b_false, map_41_4_c_true, map_41_4_a_true, map_41_4_d_null, map_41_4_c_null)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop4 DESC, n.prop3 ASC", propAsc,
        varAsc.desc(varFor("n.prop4")).asc(varFor("n.prop3")).fromLeft,
        Seq(map_40_5_c_null, map_40_5_a_true, map_40_5_b_true, map_40_5_b_false, map_41_2_b_null, map_41_2_d_null,
          map_41_4_c_null, map_41_4_d_null, map_41_4_a_true, map_41_4_c_true, map_41_4_b_false, map_41_4_c_false)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop4 DESC, n.prop3 DESC", propAsc,
        varAsc.desc(varFor("n.prop4")).desc(varFor("n.prop3")).fromLeft,
        Seq(map_40_5_c_null, map_40_5_b_true, map_40_5_a_true, map_40_5_b_false, map_41_2_d_null, map_41_2_b_null,
          map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_a_true, map_41_4_c_false, map_41_4_b_false)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 ASC, n.prop3 ASC", propDesc,
        varDesc.asc(varFor("n.prop4")).asc(varFor("n.prop3")).fromLeft,
        Seq(map_41_4_b_false, map_41_4_c_false, map_41_4_a_true, map_41_4_c_true, map_41_4_c_null, map_41_4_d_null,
          map_41_2_b_null, map_41_2_d_null, map_40_5_b_false, map_40_5_a_true, map_40_5_b_true, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 ASC, n.prop3 DESC", propDesc,
        varDesc.asc(varFor("n.prop4")).desc(varFor("n.prop3")).fromLeft,
        Seq(map_41_4_c_false, map_41_4_b_false, map_41_4_c_true, map_41_4_a_true, map_41_4_d_null, map_41_4_c_null,
          map_41_2_d_null, map_41_2_b_null, map_40_5_b_false, map_40_5_b_true, map_40_5_a_true, map_40_5_c_null)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 DESC, n.prop3 ASC", propDesc,
        varDesc.desc(varFor("n.prop4")).asc(varFor("n.prop3")).fromLeft,
        Seq(map_41_4_c_null, map_41_4_d_null, map_41_4_a_true, map_41_4_c_true, map_41_4_b_false, map_41_4_c_false,
          map_41_2_b_null, map_41_2_d_null, map_40_5_c_null, map_40_5_a_true, map_40_5_b_true, map_40_5_b_false)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 DESC, n.prop3 DESC", propDesc,
        varDesc.desc(varFor("n.prop4")).desc(varFor("n.prop3")).fromLeft,
        Seq(map_41_4_d_null, map_41_4_c_null, map_41_4_c_true, map_41_4_a_true, map_41_4_c_false, map_41_4_b_false,
          map_41_2_d_null, map_41_2_b_null, map_40_5_c_null, map_40_5_b_true, map_40_5_a_true, map_40_5_b_false))
    ).foreach {
      case (orderByString, orderIndex, sortOrder, expected) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 < 42 AND n.prop2 > 0
             |RETURN n.prop1, n.prop2, n.prop3, n.prop4
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

        // Then
        result.executionPlanDescription() should includeSomewhere
          .aPlan("PartialSort")
          .containingArgument(orderByString)
          .withOrder(sortOrder)
          .onTopOf(aPlan("Projection")
            .onTopOf(aPlan("Filter")
              .containingArgumentRegex(".*cache\\[n\\.prop2\\] > .*".r)
              .onTopOf(aPlan("NodeIndexSeek")
                .withOrder(orderIndex)
                .containingArgumentRegex("n:Label\\(prop1, prop2\\) WHERE prop1 < .* AND exists\\(prop2\\), cache\\[n.prop1\\], cache\\[n.prop2\\]".r)
              )
            )
          )

        result.toComparableResult should equal(expected)
    }
  }

  test("Order by index backed for composite index on more properties") {
    // Given
    graph.createIndex("Label", "prop1", "prop2", "prop3", "prop5")
    createNodesForComposite()

    val propAsc = ProvidedOrder
      .asc(prop("n", "prop1"))
      .asc(prop("n", "prop2"))
      .asc(prop("n", "prop3"))
      .asc(prop("n", "prop5"))
    val propDesc = ProvidedOrder
      .desc(prop("n", "prop1"))
      .desc(prop("n", "prop2"))
      .desc(prop("n", "prop3"))
      .desc(prop("n", "prop5"))

    val var1 = varFor("n.prop1")
    val var2 = varFor("n.prop2")
    val var3 = varFor("n.prop3")
    val var4 = varFor("n.prop4")
    val var5 = varFor("n.prop5")

    val map_40_5_a_true_314 = Map("n.prop1" -> 40, "n.prop2" -> 5, "n.prop3" -> "a", "n.prop4" -> true, "n.prop5" -> 3.14)
    val map_40_5_b_true_167 = Map("n.prop1" -> 40, "n.prop2" -> 5, "n.prop3" -> "b", "n.prop4" -> true, "n.prop5" -> 1.67)
    val map_40_5_c_null_272 = Map("n.prop1" -> 40, "n.prop2" -> 5, "n.prop3" -> "c", "n.prop4" -> null, "n.prop5" -> 2.72)

    val map_41_2_b_null_25 = Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> "b", "n.prop4" -> null, "n.prop5" -> 2.5)
    val map_41_2_d_null_25 = Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> "d", "n.prop4" -> null, "n.prop5" -> 2.5)

    val map_41_4_c_false_314 = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "c", "n.prop4" -> false, "n.prop5" -> 3.14)
    val map_41_4_c_null_314 = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "c", "n.prop4" -> null, "n.prop5" -> 3.14)
    val map_41_4_c_true_272 = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "c", "n.prop4" -> true, "n.prop5" -> 2.72)
    val map_41_4_d_null_167 = Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> "d", "n.prop4" -> null, "n.prop5" -> 1.67)

    Seq(
      // Order on index only
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop5 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).asc(var3).desc(var5).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272, map_41_2_b_null_25, map_41_2_d_null_25,
          map_41_4_c_false_314, map_41_4_c_null_314, map_41_4_c_true_272, map_41_4_d_null_167)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop5 ASC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).desc(var3).asc(var5).fromLeft,
        Seq(map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314, map_41_2_d_null_25, map_41_2_b_null_25,
          map_41_4_d_null_167, map_41_4_c_true_272, map_41_4_c_false_314, map_41_4_c_null_314)),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 ASC, n.prop5 ASC", propAsc,
        ProvidedOrder.asc(var1).desc(var2).asc(var3).asc(var5).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272, map_41_4_c_true_272, map_41_4_c_false_314,
          map_41_4_c_null_314, map_41_4_d_null_167, map_41_2_b_null_25, map_41_2_d_null_25)),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 DESC, n.prop5 DESC", propDesc,
        ProvidedOrder.desc(var1).asc(var2).desc(var3).desc(var5).fromLeft,
        Seq(map_41_2_d_null_25, map_41_2_b_null_25, map_41_4_d_null_167, map_41_4_c_null_314, map_41_4_c_false_314,
          map_41_4_c_true_272, map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop5 DESC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).asc(var3).desc(var5).fromLeft,
        Seq(map_41_4_c_null_314, map_41_4_c_false_314, map_41_4_c_true_272, map_41_4_d_null_167, map_41_2_b_null_25,
          map_41_2_d_null_25, map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop5 ASC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).desc(var3).asc(var5).fromLeft,
        Seq(map_41_4_d_null_167, map_41_4_c_true_272, map_41_4_c_null_314, map_41_4_c_false_314, map_41_2_d_null_25,
          map_41_2_b_null_25, map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314)),

      ("n.prop1 DESC, n.prop2 DESC, n.prop5 ASC, n.prop3 DESC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).asc(var5).desc(var3).fromLeft,
        Seq(map_41_4_d_null_167, map_41_4_c_true_272, map_41_4_c_null_314, map_41_4_c_false_314, map_41_2_d_null_25,
          map_41_2_b_null_25, map_40_5_b_true_167, map_40_5_c_null_272, map_40_5_a_true_314)),
      ("n.prop1 DESC, n.prop5 ASC, n.prop3 DESC, n.prop2 DESC", propDesc,
        ProvidedOrder.desc(var1).asc(var5).desc(var3).desc(var2).fromLeft,
        Seq(map_41_4_d_null_167, map_41_2_d_null_25, map_41_2_b_null_25, map_41_4_c_true_272, map_41_4_c_null_314,
          map_41_4_c_false_314, map_40_5_b_true_167, map_40_5_c_null_272, map_40_5_a_true_314)),

      // Order on more than index
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop5 ASC, n.prop4 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).asc(var3).asc(var5).desc(var4).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272, map_41_2_b_null_25, map_41_2_d_null_25,
          map_41_4_c_true_272, map_41_4_c_null_314, map_41_4_c_false_314, map_41_4_d_null_167)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop5 DESC, n.prop4 ASC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).asc(var3).desc(var5).asc(var4).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272, map_41_2_b_null_25, map_41_2_d_null_25,
          map_41_4_c_false_314, map_41_4_c_null_314, map_41_4_c_true_272, map_41_4_d_null_167)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop5 ASC, n.prop4 ASC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).desc(var3).asc(var5).asc(var4).fromLeft,
        Seq(map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314, map_41_2_d_null_25, map_41_2_b_null_25,
          map_41_4_d_null_167, map_41_4_c_true_272, map_41_4_c_false_314, map_41_4_c_null_314)),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 ASC, n.prop5 ASC, n.prop4 ASC", propAsc,
        ProvidedOrder.asc(var1).desc(var2).asc(var3).asc(var5).asc(var4).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272, map_41_4_c_true_272, map_41_4_c_false_314,
          map_41_4_c_null_314, map_41_4_d_null_167, map_41_2_b_null_25, map_41_2_d_null_25)),

      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC, n.prop5 ASC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).desc(var3).desc(var4).asc(var5).fromLeft,
        Seq(map_41_4_d_null_167, map_41_4_c_null_314, map_41_4_c_true_272, map_41_4_c_false_314, map_41_2_d_null_25,
          map_41_2_b_null_25, map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC, n.prop5 ASC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).desc(var3).asc(var4).asc(var5).fromLeft,
        Seq(map_41_4_d_null_167, map_41_4_c_false_314, map_41_4_c_true_272, map_41_4_c_null_314, map_41_2_d_null_25,
          map_41_2_b_null_25, map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 DESC, n.prop5 DESC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).asc(var3).desc(var4).desc(var5).fromLeft,
        Seq(map_41_4_c_null_314, map_41_4_c_true_272, map_41_4_c_false_314, map_41_4_d_null_167, map_41_2_b_null_25,
          map_41_2_d_null_25, map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272)),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 DESC, n.prop4 DESC, n.prop5 DESC", propDesc,
        ProvidedOrder.desc(var1).asc(var2).desc(var3).desc(var4).desc(var5).fromLeft,
        Seq(map_41_2_d_null_25, map_41_2_b_null_25, map_41_4_d_null_167, map_41_4_c_null_314, map_41_4_c_true_272,
          map_41_4_c_false_314, map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314)),

      // Order partially on index
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).desc(var3).fromLeft,
        Seq(map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314, map_41_2_d_null_25, map_41_2_b_null_25,
          map_41_4_d_null_167, map_41_4_c_true_272, map_41_4_c_false_314, map_41_4_c_null_314)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop5 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).desc(var5).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_c_null_272, map_40_5_b_true_167, map_41_2_b_null_25, map_41_2_d_null_25,
          map_41_4_c_false_314, map_41_4_c_null_314, map_41_4_c_true_272, map_41_4_d_null_167)),
      ("n.prop1 ASC, n.prop3 ASC, n.prop5 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var3).desc(var5).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272, map_41_2_b_null_25, map_41_4_c_false_314,
          map_41_4_c_null_314, map_41_4_c_true_272, map_41_2_d_null_25, map_41_4_d_null_167)),

      ("n.prop1 ASC, n.prop2 ASC, n.prop4 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).desc(var4).fromLeft,
        Seq(map_40_5_c_null_272, map_40_5_a_true_314, map_40_5_b_true_167, map_41_2_b_null_25, map_41_2_d_null_25,
          map_41_4_c_null_314, map_41_4_d_null_167, map_41_4_c_true_272, map_41_4_c_false_314)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).asc(var3).desc(var4).fromLeft,
        Seq(map_40_5_a_true_314, map_40_5_b_true_167, map_40_5_c_null_272, map_41_2_b_null_25, map_41_2_d_null_25,
          map_41_4_c_null_314, map_41_4_c_true_272, map_41_4_c_false_314, map_41_4_d_null_167)),
      ("n.prop1 ASC, n.prop2 ASC, n.prop5 ASC, n.prop4 DESC", propAsc,
        ProvidedOrder.asc(var1).asc(var2).asc(var5).desc(var4).fromLeft,
        Seq(map_40_5_b_true_167, map_40_5_c_null_272, map_40_5_a_true_314, map_41_2_b_null_25, map_41_2_d_null_25,
          map_41_4_d_null_167, map_41_4_c_true_272, map_41_4_c_null_314, map_41_4_c_false_314)),

      ("n.prop1 DESC, n.prop2 DESC, n.prop4 ASC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).asc(var4).fromLeft,
        Seq(map_41_4_c_false_314, map_41_4_c_true_272, map_41_4_d_null_167, map_41_4_c_null_314, map_41_2_d_null_25,
          map_41_2_b_null_25, map_40_5_b_true_167, map_40_5_a_true_314, map_40_5_c_null_272)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).desc(var3).asc(var4).fromLeft,
        Seq(map_41_4_d_null_167, map_41_4_c_false_314, map_41_4_c_true_272, map_41_4_c_null_314, map_41_2_d_null_25,
          map_41_2_b_null_25, map_40_5_c_null_272, map_40_5_b_true_167, map_40_5_a_true_314)),
      ("n.prop1 DESC, n.prop2 DESC, n.prop5 DESC, n.prop4 ASC", propDesc,
        ProvidedOrder.desc(var1).desc(var2).desc(var5).asc(var4).fromLeft,
        Seq(map_41_4_c_false_314, map_41_4_c_null_314, map_41_4_c_true_272, map_41_4_d_null_167, map_41_2_d_null_25,
          map_41_2_b_null_25, map_40_5_a_true_314, map_40_5_c_null_272, map_40_5_b_true_167))
    ).foreach {
      case (orderByString, indexOrder, sortOrder, expected) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 < 42 AND n.prop2 > 0 AND n.prop3 >= '' AND n.prop5 <= 5.5
             |RETURN n.prop1, n.prop2, n.prop3, n.prop4, n.prop5
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

        result.executionPlanDescription() should includeSomewhere
          .aPlan("PartialSort")
          .containingArgument(orderByString)
          .withOrder(sortOrder)
          .onTopOf(aPlan("Projection")
            .onTopOf(aPlan("Filter")
              .containingArgumentRegex(".*cache\\[n\\.prop2\\] > .*".r, ".*cache\\[n\\.prop3\\] >= .*".r, ".*cache\\[n\\.prop5\\] <= .*".r)
              .onTopOf(aPlan("NodeIndexSeek")
                .withOrder(indexOrder)
                .containingArgumentRegex("n:Label\\(prop1, prop2, prop3, prop5\\) WHERE prop1 < .* AND exists\\(prop2\\) AND exists\\(prop3\\) AND exists\\(prop5\\), cache\\[n.prop1\\], cache\\[n.prop2\\], cache\\[n.prop3\\], cache\\[n.prop5\\]".r)
              )
            )
          )

        result.toComparableResult should equal(expected)
    }
  }

  test("Order by index backed for composite index when not returning same as order on") {
    // Given
    graph.createIndex("Label", "prop3", "prop5")
    createNodesForComposite()

    val prop3 = prop("n", "prop3")
    val prop5 = prop("n", "prop5")
    val var3 = varFor("n.prop3")
    val var5 = varFor("n.prop5")


    val map_40_5_b_167 = Map("n.prop2" -> 5, "n.prop3" -> "b", "n.prop5" -> 1.67)
    val map_41_2_b_25 = Map("n.prop2" -> 2, "n.prop3" -> "b", "n.prop5" -> 2.5)
    val map_40_5_c_272 = Map("n.prop2" -> 5, "n.prop3" -> "c", "n.prop5" -> 2.72)
    val map_41_4_c_272 = Map("n.prop2" -> 4, "n.prop3" -> "c", "n.prop5" -> 2.72)
    val map_41_4_d_167 = Map("n.prop2" -> 4, "n.prop3" -> "d", "n.prop5" -> 1.67)
    val map_41_2_d_25 = Map("n.prop2" -> 2, "n.prop3" -> "d", "n.prop5" -> 2.5)


    Seq(
      ("n.prop3", "n.prop3 ASC, n.prop5 DESC", ProvidedOrder.asc(prop3).asc(prop5), ProvidedOrder.asc(var3).desc(prop5).fromLeft,
        Seq(Map("n.prop3" -> "b"), Map("n.prop3" -> "b"), Map("n.prop3" -> "c"), Map("n.prop3" -> "c"), Map("n.prop3" -> "d"), Map("n.prop3" -> "d"))),
      ("n.prop5", "n.prop3 DESC, n.prop5 ASC", ProvidedOrder.desc(prop3).desc(prop5), ProvidedOrder.desc(prop3).asc(var5).fromLeft,
        Seq(Map("n.prop5" -> 1.67), Map("n.prop5" -> 2.5), Map("n.prop5" -> 2.72), Map("n.prop5" -> 2.72), Map("n.prop5" -> 1.67), Map("n.prop5" -> 2.5))),

      ("n.prop3, n.prop5", "n.prop3 ASC, n.prop5 ASC, n.prop2 ASC", ProvidedOrder.asc(prop3).asc(prop5),
        ProvidedOrder.asc(var3).asc(var5).asc(prop("n", "prop2")).fromLeft,
        Seq(Map("n.prop3" -> "b", "n.prop5" -> 1.67), Map("n.prop3" -> "b", "n.prop5" -> 2.5),
          Map("n.prop3" -> "c", "n.prop5" -> 2.72), Map("n.prop3" -> "c", "n.prop5" -> 2.72),
          Map("n.prop3" -> "d", "n.prop5" -> 1.67), Map("n.prop3" -> "d", "n.prop5" -> 2.5))),
      ("n.prop3, n.prop5, n.prop2", "n.prop3 ASC, n.prop5 ASC, n.prop2 ASC, n.prop1 ASC", ProvidedOrder.asc(prop3).asc(prop5),
        ProvidedOrder.asc(var3).asc(var5).asc(varFor("n.prop2")).asc(prop("n", "prop1")).fromLeft,
        Seq(map_40_5_b_167, map_41_2_b_25, map_41_4_c_272, map_40_5_c_272, map_41_4_d_167, map_41_2_d_25)),
      ("n.prop3, n.prop5, n.prop2", "n.prop3 ASC, n.prop5 ASC, n.prop1 ASC, n.prop2 ASC", ProvidedOrder.asc(prop3).asc(prop5),
        ProvidedOrder.asc(var3).asc(var5).asc(prop("n", "prop1")).asc(varFor("n.prop2")).fromLeft,
        Seq(map_40_5_b_167, map_41_2_b_25, map_40_5_c_272, map_41_4_c_272, map_41_4_d_167, map_41_2_d_25)),
      ("n.prop3, n.prop5, n.prop2", "n.prop3 DESC, n.prop5 DESC, n.prop2 DESC, n.prop1 DESC", ProvidedOrder.desc(prop3).desc(prop5),
        ProvidedOrder.desc(var3).desc(var5).desc(varFor("n.prop2")).desc(prop("n", "prop1")).fromLeft,
        Seq(map_41_2_d_25, map_41_4_d_167, map_40_5_c_272, map_41_4_c_272, map_41_2_b_25, map_40_5_b_167)),
      ("n.prop3, n.prop5, n.prop2", "n.prop3 DESC, n.prop5 DESC, n.prop1 DESC, n.prop2 DESC", ProvidedOrder.desc(prop3).desc(prop5),
        ProvidedOrder.desc(var3).desc(var5).desc(prop("n", "prop1")).desc(varFor("n.prop2")).fromLeft,
        Seq(map_41_2_d_25, map_41_4_d_167, map_41_4_c_272, map_40_5_c_272, map_41_2_b_25, map_40_5_b_167))
    ).foreach {
      case (returnString, orderByString, indexOrder, sortOrder, expected) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop3 >= '' AND n.prop5 < 3.0
             |RETURN $returnString
             |ORDER BY $orderByString""".stripMargin
        val result = executeWith(Configs.InterpretedAndSlotted, query, executeExpectedFailures = false)

        // Then
        result.executionPlanDescription() should includeSomewhere
          .aPlan("PartialSort")
          .containingArgument(orderByString)
          .withOrder(sortOrder)
          .onTopOf(aPlan("Projection")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Filter")
                .containingArgumentRegex(".*cache\\[n\\.prop5\\] < .*".r)
                .onTopOf(aPlan("NodeIndexSeek")
                  .withOrder(indexOrder)
                  .containingArgumentRegex("n:Label\\(prop3, prop5\\) WHERE prop3 >= .* AND exists\\(prop5\\), cache\\[n.prop3\\], cache\\[n.prop5\\]".r)
                )
              )
            )
          )

        result.toComparableResult should equal(expected)
    }
  }

  // ignore order

  test("should ignore order for property with equality predicate and use order from property with range predicate") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expected = Seq(Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 1), Map("n.prop1" -> 42, "n.prop2" -> 0))
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2"))
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2"))

    Seq(
      ("n.prop1,      n.prop2 DESC", orderAscDesc, expected),
      ("n.prop1 ASC,  n.prop2 DESC", orderAscDesc, expected),
      ("n.prop1 DESC, n.prop2 ASC",  orderDescAsc, expected.reverse),
      ("n.prop1 DESC, n.prop2",      orderDescAsc, expected.reverse),
    ).foreach {
      case (orderByString, order, expected) =>
        Seq(
          "n.prop1 = 42    AND n.prop2 <= 3",
          "n.prop1 IN [42] AND n.prop2 <= $a" // no auto-parametrization to list keeps type, with single value it is the same as =
        ).foreach { predicates =>
          withClue(s"WHERE $predicates ORDER BY $orderByString:") {
            val query = s"""MATCH (n:Label)
                           |WHERE $predicates
                           |RETURN n.prop1, n.prop2
                           |ORDER BY $orderByString""".stripMargin
            // When
            val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, params = Map("a" -> 3))

            // Then
            result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
              includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

            result.toComparableResult should equal(expected)
          }
        }
    }
  }

  test("should ignore order for properties with equality predicate") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()
    createLabeledNode(Map("prop1" -> 42, "prop2" -> 3), "Label")

    val expected = Seq(Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 3))
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2"))
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2"))

    Seq(
      ("n.prop1,      n.prop2 DESC", orderAscDesc, expected),
      ("n.prop1 ASC,  n.prop2 DESC", orderAscDesc, expected),
      ("n.prop1 DESC, n.prop2 ASC",  orderDescAsc, expected),
      ("n.prop1 DESC, n.prop2",      orderDescAsc, expected),
    ).foreach {
      case (orderByString, order, expected) =>
        withClue(s"ORDER BY $orderByString:") {
          val query = s"""MATCH (n:Label)
                        |WHERE n.prop1 = 42 AND n.prop2 = 3
                        |RETURN n.prop1, n.prop2
                        |ORDER BY $orderByString""".stripMargin

          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for properties with equality predicates with multiple properties (equality predicates only)") {
    // Given
    graph.createIndex("Label", "prop1", "prop2", "prop3")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 2, "prop3" -> 1.67), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 1.67), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 2.72), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 3.14), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 3.14), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 5, "prop3" -> 1.67), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 5, "prop3" -> 9.82), "Label")

    val orderAscAscDesc = ProvidedOrder.asc(varFor("n.prop1")).asc(varFor("n.prop2")).desc(varFor("n.prop3"))
    val orderAscDescAsc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2")).asc(varFor("n.prop3"))
    val orderAscDescDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2")).desc(varFor("n.prop3"))

    val orderDescDescAsc = ProvidedOrder.desc(varFor("n.prop1")).desc(varFor("n.prop2")).asc(varFor("n.prop3"))
    val orderDescAscDesc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2")).desc(varFor("n.prop3"))
    val orderDescAscAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2")).asc(varFor("n.prop3"))

    Seq(
      ("n.prop1, n.prop2, n.prop3 DESC", orderAscAscDesc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1, n.prop2 DESC, n.prop3", orderAscDescAsc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1, n.prop2 DESC, n.prop3 DESC", orderAscDescDesc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", orderAscAscDesc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", orderAscDescAsc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 DESC", orderAscDescDesc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", orderDescDescAsc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 DESC", orderDescAscDesc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", orderDescAscAsc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3", orderDescDescAsc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 DESC, n.prop2, n.prop3 DESC", orderDescAscDesc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
      ("n.prop1 DESC, n.prop2, n.prop3", orderDescAscAsc, Seq(Map("n.prop1" -> 41, "n.prop2" -> 2, "n.prop3" -> 1.67))),
    ).foreach {
      case (orderByString, order, expected) =>
        withClue(s"ORDER BY $orderByString:") {
          val query = s"""MATCH (n:Label)
                         |WHERE n.prop1 = 41 AND n.prop2 = 2 AND n.prop3 = 1.67
                         |RETURN n.prop1, n.prop2, n.prop3
                         |ORDER BY $orderByString""".stripMargin
          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for properties with equality predicates with multiple properties") {
    // Given
    graph.createIndex("Label", "prop1", "prop2", "prop3")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 2, "prop3" -> 1.67), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 1.67), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 2.72), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 3.14), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 4, "prop3" -> 3.14), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 5, "prop3" -> 1.67), "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 5, "prop3" -> 9.82), "Label")

    val expectedLessThen3 = Seq(Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> 2.72), Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> 1.67))
    val expectedMoreThen3 = Seq(Map("n.prop1" -> 41, "n.prop2" -> 5, "n.prop3" -> 1.67), Map("n.prop1" -> 41, "n.prop2" -> 4, "n.prop3" -> 1.67))

    val orderAscAscDesc = ProvidedOrder.asc(varFor("n.prop1")).asc(varFor("n.prop2")).desc(varFor("n.prop3"))
    val orderAscDescAsc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2")).asc(varFor("n.prop3"))
    val orderAscDescDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2")).desc(varFor("n.prop3"))

    val orderDescDescAsc = ProvidedOrder.desc(varFor("n.prop1")).desc(varFor("n.prop2")).asc(varFor("n.prop3"))
    val orderDescAscDesc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2")).desc(varFor("n.prop3"))
    val orderDescAscAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2")).asc(varFor("n.prop3"))

    Seq(
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1, n.prop2, n.prop3 DESC", orderAscAscDesc, expectedLessThen3),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1, n.prop2 DESC, n.prop3", orderAscDescAsc, expectedLessThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1, n.prop2 DESC, n.prop3 DESC", orderAscDescDesc, expectedLessThen3),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", orderAscAscDesc, expectedLessThen3),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", orderAscDescAsc, expectedLessThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 ASC, n.prop2 DESC, n.prop3 DESC", orderAscDescDesc, expectedLessThen3),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", orderDescDescAsc, expectedLessThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 DESC, n.prop2 ASC, n.prop3 DESC", orderDescAscDesc, expectedLessThen3),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", orderDescAscAsc, expectedLessThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 DESC, n.prop2 DESC, n.prop3", orderDescDescAsc, expectedLessThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 DESC, n.prop2, n.prop3 DESC", orderDescAscDesc, expectedLessThen3),
      ("n.prop1 = 41 AND n.prop2 = 4 AND n.prop3 < 3", "n.prop1 DESC, n.prop2, n.prop3", orderDescAscAsc, expectedLessThen3.reverse),

      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1, n.prop2, n.prop3 DESC", orderAscAscDesc, expectedMoreThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1, n.prop2 DESC, n.prop3", orderAscDescAsc, expectedMoreThen3),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1, n.prop2 DESC, n.prop3 DESC", orderAscDescDesc, expectedMoreThen3),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", orderAscAscDesc, expectedMoreThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", orderAscDescAsc, expectedMoreThen3),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 ASC, n.prop2 DESC, n.prop3 DESC", orderAscDescDesc, expectedMoreThen3),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", orderDescDescAsc, expectedMoreThen3),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 DESC, n.prop2 ASC, n.prop3 DESC", orderDescAscDesc, expectedMoreThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", orderDescAscAsc, expectedMoreThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 DESC, n.prop2 DESC, n.prop3", orderDescDescAsc, expectedMoreThen3),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 DESC, n.prop2, n.prop3 DESC", orderDescAscDesc, expectedMoreThen3.reverse),
      ("n.prop1 = 41 AND n.prop2 > 3 AND n.prop3 = 1.67", "n.prop1 DESC, n.prop2, n.prop3", orderDescAscAsc, expectedMoreThen3.reverse),
    ).foreach {
      case (predicates, orderByString, order, expected) =>
        withClue(s"WHERE $predicates ORDER BY $orderByString:") {
          val query = s"""MATCH (n:Label)
                         |WHERE $predicates
                         |RETURN n.prop1, n.prop2, n.prop3
                         |ORDER BY $orderByString""".stripMargin
          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for property with equality predicate and projections") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    val n0 = createLabeledNode(Map("prop1" -> 42, "prop2" -> 3), "Label")
    val n1 = createLabeledNode(Map("prop1" -> 42, "prop2" -> 1), "Label")
    val n2 = createLabeledNode(Map("prop1" -> 42, "prop2" -> 0), "Label")

    val expectedM = Seq(Map("m" -> n0), Map("m" -> n1), Map("m" -> n2))
    val expectedNoN = Seq(Map("prop1" -> 42, "prop2" -> 3), Map("prop1" -> 42, "prop2" -> 1), Map("prop1" -> 42, "prop2" -> 0))
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2"))
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2"))

    Seq(
      ("RETURN n AS m                             ORDER BY m.prop1, m.prop2 DESC", orderAscDesc, expectedM),
      ("RETURN n.prop1 AS prop1, n.prop2 AS prop2 ORDER BY prop1 ASC, prop2 DESC", orderAscDesc, expectedNoN),
      ("RETURN n AS m                             ORDER BY m.prop1 DESC, m.prop2", orderDescAsc, expectedM.reverse),
      ("RETURN n.prop1 AS prop1, n.prop2 AS prop2 ORDER BY prop1 DESC, prop2 ASC", orderDescAsc, expectedNoN.reverse),
    ).foreach {
      case (returnAndOrderBy, order, expected) =>
        withClue(s"$returnAndOrderBy:") {
          val query = s"""MATCH (n:Label)
                         |WHERE n.prop1 = 42 AND n.prop2 <= 3
                         |$returnAndOrderBy""".stripMargin
          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for properties with equality predicates in WITH") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    val n0 = createLabeledNode(Map("prop1" -> 42, "prop2" -> 3), "Label")
    val n1 = createLabeledNode(Map("prop1" -> 42, "prop2" -> 1), "Label")
    val n2 = createLabeledNode(Map("prop1" -> 42, "prop2" -> 0), "Label")

    val expectedM = Seq(Map("m" -> n0), Map("m" -> n1), Map("m" -> n2))
    val expectedN = Seq(Map("n" -> n0, "ignored" -> 1), Map("n" -> n1, "ignored" -> 1), Map("n" -> n2, "ignored" -> 1))
    val expectedNoN = Seq(Map("prop1" -> 42, "prop2" -> 3), Map("prop1" -> 42, "prop2" -> 1), Map("prop1" -> 42, "prop2" -> 0))
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2"))
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2"))

    Seq(
      ("WITH n, 1 AS ignored                    ORDER BY n.prop1, n.prop2 DESC", orderAscDesc, expectedN),
      ("WITH n AS m                             ORDER BY m.prop1, m.prop2 DESC", orderAscDesc, expectedM),
      ("WITH n.prop1 AS prop1, n.prop2 AS prop2 ORDER BY prop1 ASC, prop2 DESC", orderAscDesc, expectedNoN),
      ("WITH n, 1 AS ignored                    ORDER BY n.prop1 DESC, n.prop2", orderDescAsc, expectedN.reverse),
      ("WITH n AS m                             ORDER BY m.prop1 DESC, m.prop2", orderDescAsc, expectedM.reverse),
      ("WITH n.prop1 AS prop1, n.prop2 AS prop2 ORDER BY prop1 DESC, prop2 ASC", orderDescAsc, expectedNoN.reverse),
    ).foreach {
      case (withAndOrderBy, order, expected) =>
        withClue(s"ORDER BY $withAndOrderBy:") {
          val query = s"""MATCH (n:Label)
                         |WHERE n.prop1 = 42 AND n.prop2 <= 3
                         |$withAndOrderBy
                         |RETURN *""".stripMargin
          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for properties with equality predicates when renamed multiple times") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expected = Seq(Map("prop1" -> 42, "prop2" -> 0), Map("prop1" -> 42, "prop2" -> 1), Map("prop1" -> 42, "prop2" -> 3))
    val order = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2"))

    Seq(
      ("WITH n.prop1 AS prop1, n.prop2 AS prop2, 1 AS ignored RETURN *", expected.map(_ ++ Map("ignored" -> 1))),
      ("WITH n.prop1 AS prop1, n.prop2 AS prop2, 1 AS ignored RETURN prop1, prop2", expected),
      ("WITH n.prop1 AS prop1, n.prop2 AS prop2, 1 AS ignored WITH prop1 AS prop1, prop2 AS prop2 RETURN *", expected),
      ("WITH n.prop1 AS prop1, n.prop2 AS prop2, 1 AS ignored WITH prop1 AS prop1, prop2 AS prop2 RETURN prop1, prop2", expected),
    ).foreach {
      case (middle, expected) =>
        withClue(s"ORDER BY $middle:") {
          val query = s"""MATCH (n:Label)
                         |WHERE n.prop1 = 42 AND n.prop2 <= 3
                         |$middle
                         |ORDER BY prop1 DESC, prop2 ASC""".stripMargin
          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for property with equality predicate when its the second property") {
    // Given
    graph.createIndex("Label", "prop2", "prop3")
    createNodesForComposite()
    createLabeledNode(Map("prop2" -> 2, "prop3" -> "f"), "Label")

    val expected = Seq(Map("n.prop2" -> 1, "n.prop3" -> "f"), Map("n.prop2" -> 2, "n.prop3" -> "f"))
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop2")).asc(varFor("n.prop3")).fromLeft
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop2")).desc(varFor("n.prop3")).fromLeft

    Seq(
      ("n.prop2,      n.prop3 DESC", orderAscDesc, expected),
      ("n.prop2 ASC,  n.prop3 DESC", orderAscDesc, expected),
      ("n.prop2 DESC, n.prop3 ASC",  orderDescAsc, expected.reverse),
      ("n.prop2 DESC, n.prop3",      orderDescAsc, expected.reverse),
    ).foreach {
      case (orderByString, order, expected) =>
        withClue(s"ORDER BY $orderByString:") {
          val query = s"""MATCH (n:Label)
                         |WHERE n.prop2 < 3 AND n.prop3 = 'f'
                         |RETURN n.prop2, n.prop3
                         |ORDER BY $orderByString""".stripMargin

          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(order))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should use partial sort when there is a list in predicate") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expectedEquals = Seq(Map("n.prop1" -> 44, "n.prop2" -> 3))
    val expectedAscDesc = Seq(Map("n.prop1" -> 43, "n.prop2" -> 2), Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 44, "n.prop2" -> 3))
    val expectedDescAsc = Seq(Map("n.prop1" -> 44, "n.prop2" -> 3), Map("n.prop1" -> 43, "n.prop2" -> 1), Map("n.prop1" -> 43, "n.prop2" -> 2))
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2")).fromLeft
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2")).fromLeft

    Seq(
      ("n.prop1 in [43, 44] AND n.prop2 in [3]",       "n.prop1, n.prop2 DESC",     orderAscDesc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [3, 3]",    "n.prop1, n.prop2 DESC",     orderAscDesc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [1, 2, 3]", "n.prop1, n.prop2 DESC",     orderAscDesc, expectedAscDesc),
      ("n.prop1 in [43, 44] AND n.prop2 <= 3",         "n.prop1, n.prop2 DESC",     orderAscDesc, expectedAscDesc),

      ("n.prop1 in [43, 44] AND n.prop2 in [3]",       "n.prop1 ASC, n.prop2 DESC", orderAscDesc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [3, 3]",    "n.prop1 ASC, n.prop2 DESC", orderAscDesc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [1, 2, 3]", "n.prop1 ASC, n.prop2 DESC", orderAscDesc, expectedAscDesc),
      ("n.prop1 in [43, 44] AND n.prop2 <= 3",         "n.prop1 ASC, n.prop2 DESC", orderAscDesc, expectedAscDesc),

      ("n.prop1 in [43, 44] AND n.prop2 in [3]",          "n.prop1 DESC, n.prop2 ASC", orderDescAsc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [3, 3]",    "n.prop1 DESC, n.prop2 ASC", orderDescAsc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [1, 2, 3]", "n.prop1 DESC, n.prop2 ASC", orderDescAsc, expectedDescAsc),
      ("n.prop1 in [43, 44] AND n.prop2 <= 3",         "n.prop1 DESC, n.prop2 ASC", orderDescAsc, expectedDescAsc),

      ("n.prop1 in [43, 44] AND n.prop2 in [3]",          "n.prop1 DESC, n.prop2",     orderDescAsc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [3, 3]",    "n.prop1 DESC, n.prop2",     orderDescAsc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 in [1, 2, 3]", "n.prop1 DESC, n.prop2",     orderDescAsc, expectedDescAsc),
      ("n.prop1 in [43, 44] AND n.prop2 <= 3",         "n.prop1 DESC, n.prop2",     orderDescAsc, expectedDescAsc),
    ).foreach {
      case (predicate, orderByString, order, expected) =>
        withClue(s"WHERE $predicate ORDER BY $orderByString:") {
          val query = s"""MATCH (n:Label)
                         |WHERE $predicate
                         |RETURN n.prop1, n.prop2
                         |ORDER BY $orderByString""".stripMargin

          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (includeSomewhere.aPlan("PartialSort").withOrder(order) and
            includeSomewhere.aPlan("NodeIndexSeek"))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should use index order when we have one list predicate and one exact predicate") {
    // Given
    graph.createIndex("Label", "prop1", "prop2")
    createNodesForComposite()

    val expectedEquals = Seq(Map("n.prop1" -> 44, "n.prop2" -> 3))
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop2")).fromLeft
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop1")).asc(varFor("n.prop2")).fromLeft

    Seq(
      ("n.prop1 in [43, 44] AND n.prop2 = 3",          "n.prop1, n.prop2 DESC",     orderAscDesc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 = 3",          "n.prop1 ASC, n.prop2 DESC", orderAscDesc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 = 3",          "n.prop1 DESC, n.prop2 ASC", orderDescAsc, expectedEquals),
      ("n.prop1 in [43, 44] AND n.prop2 = 3",          "n.prop1 DESC, n.prop2",     orderDescAsc, expectedEquals),
    ).foreach {
      case (predicate, orderByString, order, expected) =>
        withClue(s"WHERE $predicate ORDER BY $orderByString:") {
          val query = s"""MATCH (n:Label)
                         |WHERE $predicate
                         |RETURN n.prop1, n.prop2
                         |ORDER BY $orderByString""".stripMargin

          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (not(includeSomewhere.aPlan("Sort")) and not(includeSomewhere.aPlan("PartialSort")) and
            includeSomewhere.aPlan("NodeIndexSeek"))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for property with equality predicate in index even when order by expression, still needs to sort on expression") {
    // Given
    graph.createIndex("Label", "prop1", "prop3")
    createNodesForComposite()

    // Both properties have equality predicates but only the second property matches the ORDER BY
    // Therefore only the second property can have switched order, not the first (which will always be ASC)
    val indexOrderAscDesc = ProvidedOrder.asc(varFor("n.prop1")).desc(varFor("n.prop3"))
    val indexOrderAscAsc = ProvidedOrder.asc(varFor("n.prop1")).asc(varFor("n.prop3"))

    def orderAscDesc(sort: String) = ProvidedOrder.asc(varFor(sort)).desc(varFor("n.prop3")).fromLeft
    def orderDescAsc(sort: String) = ProvidedOrder.desc(varFor(sort)).asc(varFor("n.prop3")).fromLeft

    Seq(
      ("n.prop1, n.prop3",            "n.prop1 * 2, n.prop3 DESC", indexOrderAscDesc, orderAscDesc("n.prop1 * $autoint_2"), Seq(Map("n.prop1" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 AS foo, n.prop3",     "foo - 2,     n.prop3 DESC", indexOrderAscDesc, orderAscDesc("foo - $autoint_2"),     Seq(Map("foo" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 * 2, n.prop3",        "n.prop1 * 2, n.prop3 DESC", indexOrderAscDesc, orderAscDesc("n.prop1 * 2"),          Seq(Map("n.prop1 * 2" -> 80, "n.prop3" -> "a"))),
      ("n.prop1 + 2 AS foo, n.prop3", "foo,         n.prop3 DESC", indexOrderAscDesc, orderAscDesc("foo"),                  Seq(Map("foo" -> 42, "n.prop3" -> "a"))),

      ("n.prop1, n.prop3",            "n.prop1 * 2 ASC, n.prop3 DESC", indexOrderAscDesc, orderAscDesc("n.prop1 * $autoint_2"), Seq(Map("n.prop1" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 AS foo, n.prop3",     "foo - 2 ASC,     n.prop3 DESC", indexOrderAscDesc, orderAscDesc("foo - $autoint_2"),     Seq(Map("foo" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 * 2, n.prop3",        "n.prop1 * 2 ASC, n.prop3 DESC", indexOrderAscDesc, orderAscDesc("n.prop1 * 2"),          Seq(Map("n.prop1 * 2" -> 80, "n.prop3" -> "a"))),
      ("n.prop1 + 2 AS foo, n.prop3", "foo ASC,         n.prop3 DESC", indexOrderAscDesc, orderAscDesc("foo"),                  Seq(Map("foo" -> 42, "n.prop3" -> "a"))),

      ("n.prop1, n.prop3",            "n.prop1 * 2 DESC, n.prop3 ASC", indexOrderAscAsc, orderDescAsc("n.prop1 * $autoint_2"), Seq(Map("n.prop1" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 AS foo, n.prop3",     "foo - 2 DESC,     n.prop3 ASC", indexOrderAscAsc, orderDescAsc("foo - $autoint_2"),     Seq(Map("foo" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 * 2, n.prop3",        "n.prop1 * 2 DESC, n.prop3 ASC", indexOrderAscAsc, orderDescAsc("n.prop1 * 2"),          Seq(Map("n.prop1 * 2" -> 80, "n.prop3" -> "a"))),
      ("n.prop1 + 2 AS foo, n.prop3", "foo DESC,         n.prop3 ASC", indexOrderAscAsc, orderDescAsc("foo"),                  Seq(Map("foo" -> 42, "n.prop3" -> "a"))),

      ("n.prop1, n.prop3",            "n.prop1 * 2 DESC, n.prop3", indexOrderAscAsc, orderDescAsc("n.prop1 * $autoint_2"), Seq(Map("n.prop1" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 AS foo, n.prop3",     "foo - 2 DESC,     n.prop3", indexOrderAscAsc, orderDescAsc("foo - $autoint_2"),     Seq(Map("foo" -> 40, "n.prop3" -> "a"))),
      ("n.prop1 * 2, n.prop3",        "n.prop1 * 2 DESC, n.prop3", indexOrderAscAsc, orderDescAsc("n.prop1 * 2"),          Seq(Map("n.prop1 * 2" -> 80, "n.prop3" -> "a"))),
      ("n.prop1 + 2 AS foo, n.prop3", "foo DESC,         n.prop3", indexOrderAscAsc, orderDescAsc("foo"),                  Seq(Map("foo" -> 42, "n.prop3" -> "a"))),
    ).foreach {
      case (returnString, orderByString, orderIndex, orderSort, expected) =>
        withClue(s"RETURN $returnString ORDER BY $orderByString:") {
          val query = s"""MATCH (n:Label)
                         |WHERE n.prop1 = 40 AND n.prop3 = 'a'
                         |RETURN $returnString
                         |ORDER BY $orderByString""".stripMargin

          // When
          val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

          // Then
          result.executionPlanDescription() should (includeSomewhere.aPlan("Sort").withOrder(orderSort) and
            includeSomewhere.aPlan("NodeIndexSeek").withOrder(orderIndex))

          result.toComparableResult should equal(expected)
        }
    }
  }

  test("should ignore order for property with multiple predicates, when rewritten away or using additional filter") {
    // Given
    graph.createIndex("Label", "prop2", "prop3")
    createNodesForComposite()

    val expected = Seq(Map("n.prop2" -> 2, "n.prop3" -> "b"), Map("n.prop2" -> 2, "n.prop3" -> "d"), Map("n.prop2" -> 2, "n.prop3" -> "g"))
    val expectedAsc = (x: Seq[Map[String, Any]]) => x
    val expectedDesc = (x: Seq[Map[String, Any]]) => x.reverse
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop2")).asc(varFor("n.prop3")).fromLeft
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop2")).desc(varFor("n.prop3")).fromLeft

    Seq(
      ("n.prop2,      n.prop3 DESC", orderAscDesc, expectedDesc),
      ("n.prop2 ASC,  n.prop3 DESC", orderAscDesc, expectedDesc),
      ("n.prop2 DESC, n.prop3 ASC",  orderDescAsc, expectedAsc),
      ("n.prop2 DESC, n.prop3",      orderDescAsc, expectedAsc),
    ).foreach {
      case (orderByString, orderIndex, expectedOrder) =>
        Seq(
          ("(n.prop2 < 3 AND n.prop2 = 2) AND n.prop3 > 'a'", true,  expectedOrder(expected)), // chooses = in index search then filters on <
          ("(n.prop2 = 2 AND n.prop2 = 2) AND n.prop3 > 'a'", false, expectedOrder(expected)), // rewritten to only one equals
          ("(n.prop2 = 2 AND n.prop2 = 3) AND n.prop3 > 'a'", true,  Seq.empty),               // chooses one of the = in index search then filters on the other
          ("(n.prop2 = 2 OR n.prop2 = 2) AND n.prop3 > 'a'",  false, expectedOrder(expected)), // rewritten to only one equals
        ).foreach {
          case (predicates, shouldFilter, expected) =>
            withClue(s"WHERE $predicates ORDER BY $orderByString:") {
              val query = s"""MATCH (n:Label)
                             |WHERE $predicates
                             |RETURN n.prop2, n.prop3
                             |ORDER BY $orderByString""".stripMargin

              // When
              val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

              // Then
              result.executionPlanDescription() should (not(includeSomewhere.aPlan("PartialSort")) and
                includeSomewhere.aPlan("NodeIndexSeek").withOrder(orderIndex))

              if (shouldFilter) {
                result.executionPlanDescription() should includeSomewhere.aPlan("Filter")
              } else {
                result.executionPlanDescription() should not(includeSomewhere.aPlan("Filter"))
              }

              result.toComparableResult should equal(expected)
            }
        }
    }
  }

  test("should not ignore order for property with multiple equalities") {
    // Given
    graph.createIndex("Label", "prop2", "prop3")
    createNodesForComposite()

    val expectedDescAsc = Seq(Map("n.prop2" -> 2, "n.prop3" -> "b"), Map("n.prop2" -> 2, "n.prop3" -> "d"), Map("n.prop2" -> 2, "n.prop3" -> "g"), Map("n.prop2" -> 1, "n.prop3" -> "f"))
    val expectedAscDesc = Seq(Map("n.prop2" -> 1, "n.prop3" -> "f"), Map("n.prop2" -> 2, "n.prop3" -> "g"), Map("n.prop2" -> 2, "n.prop3" -> "d"), Map("n.prop2" -> 2, "n.prop3" -> "b"))
    val orderDesc = ProvidedOrder.desc(varFor("n.prop2")).desc(varFor("n.prop3"))
    val orderAsc = ProvidedOrder.asc(varFor("n.prop2")).asc(varFor("n.prop3"))
    val orderDescAsc = ProvidedOrder.desc(varFor("n.prop2")).asc(varFor("n.prop3")).fromLeft
    val orderAscDesc = ProvidedOrder.asc(varFor("n.prop2")).desc(varFor("n.prop3")).fromLeft

    Seq(
      ("n.prop2,      n.prop3 DESC", orderAsc,  orderAscDesc, expectedAscDesc),
      ("n.prop2 ASC,  n.prop3 DESC", orderAsc,  orderAscDesc, expectedAscDesc),
      ("n.prop2 DESC, n.prop3 ASC",  orderDesc, orderDescAsc, expectedDescAsc),
      ("n.prop2 DESC, n.prop3",      orderDesc, orderDescAsc, expectedDescAsc),
    ).foreach {
      case (orderByString, indexOrder, sortOrder, expected) =>
        Seq(
          "(n.prop2 = 1 OR n.prop2 = 2) AND n.prop3 > 'a'",
          "n.prop2 IN [1, 2] AND n.prop3 > $a",
        ).foreach { predicates =>
          withClue(s"WHERE $predicates ORDER BY $orderByString:") {
            val query = s"""MATCH (n:Label)
                           |WHERE $predicates
                           |RETURN n.prop2, n.prop3
                           |ORDER BY $orderByString""".stripMargin

            // When
            val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, params = Map("a" -> "a"))

            // Then
            result.executionPlanDescription() should (includeSomewhere.aPlan("PartialSort").withOrder(sortOrder) and
              includeSomewhere.aPlan("NodeIndexSeek").withOrder(indexOrder))

            result.toComparableResult should equal(expected)
          }
        }
    }
  }

  // Min and Max

  for ((TestOrder(cypherToken, expectedOrder, providedOrder), functionName) <- List((ASCENDING, "min"), (DESCENDING, "max"))) {
    test(s"$cypherToken-$functionName: should use provided index order with range") {
      val result = executeWith(Configs.Optional,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1").withOrder(providedOrder(prop("n", "prop1"))))
            )
          )

      val expected = expectedOrder(List(
        Map("min(n.prop1)" -> 40),
        Map("max(n.prop1)" -> 44)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order for multiple types") {
      graph.withTx( tx => {
        tx.execute("CREATE (:Awesome {prop1: 'hallo'})")
        tx.execute("CREATE (:Awesome {prop1: 35.5})")
      })

      val result = executeWith(Configs.Optional,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1").withOrder(providedOrder(prop("n", "prop1"))))
            )
          )

      val expected = expectedOrder(List(
        Map("min(n.prop1)" -> 35.5),
        Map("max(n.prop1)" -> 44)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order with renamed property") {
      val result = executeWith(Configs.Optional,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n.prop1 AS prop RETURN $functionName(prop) AS extreme", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1").withOrder(providedOrder(prop("n", "prop1"))))
              )
            )
          )

      val expected = expectedOrder(List(
        Map("extreme" -> 40), // min
        Map("extreme" -> 44) // max
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order with renamed property (named index)") {
      graph.withTx(tx => createNodesForNamedIndex(tx))
      graph.createIndexWithName("my_index", "Label", "prop")

      val result = executeWith(Configs.Optional,
        s"MATCH (n:Label) WHERE n.prop > 0 WITH n.prop AS prop RETURN $functionName(prop) AS extreme", executeBefore = createNodesForNamedIndex)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop").withOrder(providedOrder(prop("n", "prop"))))
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
      val result = executeWith(Configs.Optional,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n as m RETURN $functionName(m.prop1) AS extreme", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1").withOrder(providedOrder(prop("n", "prop1"))))
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
      val result = executeWith(Configs.Optional,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n as m WITH m.prop1 AS prop RETURN $functionName(prop) AS extreme", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("Projection")
                  .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1").withOrder(providedOrder(prop("n", "prop1"))))
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

      val result = executeWith(Configs.Optional,
        s"MATCH (n: B) WHERE n.prop > 0 RETURN $functionName(n.prop)", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop").withOrder(providedOrder(prop("n", "prop"))))
            )
          )

      val expected = List(Map(s"$functionName(n.prop)" -> null))
      result.toList should equal(expected)
    }

    test(s"$cypherToken-$functionName: should use provided index order with ORDER BY on same property") {
      val result = executeWith(Configs.Optional,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1) ORDER BY $functionName(n.prop1) $cypherToken", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1").withOrder(providedOrder(prop("n", "prop1"))))
            )
          )

      val expected = expectedOrder(List(
        Map("min(n.prop1)" -> 40),
        Map("max(n.prop1)" -> 44)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: cannot use provided index order for prop2 when ORDER BY prop1") {
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 WITH n.prop1 AS prop1, n.prop2 as prop2 ORDER BY prop1 RETURN $functionName(prop2)",
        executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = expectedOrder(List(
        Map("min(prop2)" -> 1),
        Map("max(prop2)" -> 5)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should use provided index order for nested functions with $functionName as inner") {
      graph.withTx( tx => tx.execute("CREATE (:Awesome {prop3: 'ha'})"))

      //should give the length of the alphabetically smallest/largest prop3 string
      val result = executeWith(Configs.Optional,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN size($functionName(n.prop3)) AS agg", executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("Projection")
          .onTopOf(aPlan("Optional")
            .onTopOf(aPlan("Limit")
              .onTopOf(aPlan("Projection")
                .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("n").containingArgumentForCachedProperty("n", "prop3").withOrder(providedOrder(prop("n", "prop3"))))
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
      graph.withTx( tx => tx.execute("CREATE (:Awesome {prop3: 'ha'})"))

      //should give the length of the shortest/longest prop3 string
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN $functionName(size(n.prop3)) AS agg", executeBefore = createSomeNodes)

      val expected = expectedOrder(List(
        Map("agg" -> 2), // min: ha
        Map("agg" -> 9) // max: footurama
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should give correct result for nested functions in several depths") {
      graph.withTx( tx => tx.execute("CREATE (:Awesome {prop3: 'ha'})"))

      //should give the length of the shortest/longest prop3 string
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) WHERE n.prop3 > '' RETURN $functionName(size(trim(replace(n.prop3, 'a', 'aa')))) AS agg",
        executeBefore = createSomeNodes)

      val expected = expectedOrder(List(
        Map("agg" -> 3), // min: haa
        Map("agg" -> 11) // max: footuraamaa
      )).head
      result.toList should equal(List(expected))
    }

    // The planer doesn't yet support composite range scans, so we can't avoid the aggregation
    // TODO fix so we can avoid the aggregation now that range scans are handled
    test(s"$cypherToken-$functionName: cannot use provided index order from composite index without filter") {
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) WHERE n.prop1 = 40 AND n.prop2 < 4 RETURN $functionName(n.prop1), $functionName(n.prop2)",
        executeBefore = createMoreNodes)

      result.executionPlanDescription() should includeSomewhere.aPlan("EagerAggregation")

      val expected = expectedOrder(List(
        Map("min(n.prop1)" -> 40, "min(n.prop2)" -> 0),
        Map("max(n.prop1)" -> 40, "max(n.prop2)" -> 3)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: cannot use provided index order from composite index with filter") {
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) WHERE n.prop1 > 40 AND n.prop2 > 0 RETURN $functionName(n.prop1), $functionName(n.prop2)",
        executeBefore = createMoreNodes)

      result.executionPlanDescription() should includeSomewhere.aPlan("EagerAggregation")

      val expected = expectedOrder(List(
        Map("min(n.prop1)" -> 41, "min(n.prop2)" -> 1),
        Map("max(n.prop1)" -> 44, "max(n.prop2)" -> 4)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: cannot use provided index order with multiple aggregations") {
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1), count(n.prop1)", executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = expectedOrder(List(
        Map("min(n.prop1)" -> 40, "count(n.prop1)" -> 10),
        Map("max(n.prop1)" -> 44, "count(n.prop1)" -> 10)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: cannot use provided index order with grouping expression (caused by return n.prop2)") {
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) WHERE n.prop1 > 0 RETURN $functionName(n.prop1), n.prop2", executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = functionName match {
        case "min" =>
          Set(
            Map("min(n.prop1)" -> 43, "n.prop2" -> 1),
            Map("min(n.prop1)" -> 41, "n.prop2" -> 2),
            Map("min(n.prop1)" -> 42, "n.prop2" -> 3),
            Map("min(n.prop1)" -> 40, "n.prop2" -> 5)
          )
        case "max" =>
          Set(
            Map("max(n.prop1)" -> 43, "n.prop2" -> 1),
            Map("max(n.prop1)" -> 41, "n.prop2" -> 2),
            Map("max(n.prop1)" -> 44, "n.prop2" -> 3),
            Map("max(n.prop1)" -> 40, "n.prop2" -> 5)
          )
      }

      result.toSet should equal(expected)
    }

    test(s"$cypherToken-$functionName: should plan aggregation for index scan") {
      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:Awesome) RETURN $functionName(n.prop1)", executeBefore = createSomeNodes)
      result.executionPlanDescription() should
        includeSomewhere.aPlan("Optional")
          .onTopOf(aPlan("Limit")
            .onTopOf(aPlan("Projection")
              .onTopOf(aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1").withOrder(providedOrder(prop("n", "prop1"))))
            )
          )

      val expected = expectedOrder(List(
        Map("min(n.prop1)" -> 40),
        Map("max(n.prop1)" -> 44)
      )).head
      result.toList should equal(List(expected))
    }

    test(s"$cypherToken-$functionName: should plan aggregation without index") {

      createLabeledNode(Map("foo" -> 2), "B")

      val result = executeWith(Configs.CachedProperty,
        s"MATCH (n:B) WHERE n.foo > 0 RETURN $functionName(n.foo)", executeBefore = createSomeNodes)

      val plan = result.executionPlanDescription()
      plan should includeSomewhere.aPlan("NodeByLabelScan")
      plan should includeSomewhere.aPlan("EagerAggregation")

      val expected = functionName match {
        case "min" => Set(Map("min(n.foo)" -> 1))
        case "max" => Set(Map("max(n.foo)" -> 2))
      }
      result.toSet should equal(expected)
    }

    test(s"$cypherToken:  should use index order for range predicate with parameter") {
      val query = s"MATCH (n:Awesome) WHERE n.prop2 > $$param RETURN n.prop2 ORDER BY n.prop2 $cypherToken"
      val result = executeWith(Configs.InterpretedAndSlotted + Configs.Pipelined, query, executeBefore = createSomeNodes, params = Map("param" -> 1))

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
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

    test(s"$cypherToken:  should use index order for range predicate with string parameter") {
      graph.withTx( tx => createStringyNodes(tx))
      val query = s"MATCH (n:Awesome) WHERE n.prop3 STARTS WITH $$param RETURN n.prop3 ORDER BY n.prop3 $cypherToken"
      val result = executeWith(Configs.InterpretedAndSlotted + Configs.Pipelined, query, executeBefore = createSomeNodes, params = Map("param" -> "s"))

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop3" -> "scat"), Map("n.prop3" -> "scratch")
      )))
    }

    test(s"$cypherToken:  should use index order for range predicate with multiple of the same string parameter") {
      graph.withTx( tx => createStringyNodes(tx))
      val query = s"MATCH (n:Awesome) WHERE n.prop3 STARTS WITH $$param AND NOT n.prop3 ENDS WITH $$param RETURN n.prop3 ORDER BY n.prop3 $cypherToken"
      val result = executeWith(Configs.InterpretedAndSlotted + Configs.Pipelined, query, executeBefore = createSomeNodes, params = Map("param" -> "s"))

      result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
      result.toList should be(expectedOrder(List(
        Map("n.prop3" -> "scat"), Map("n.prop3" -> "scratch")
      )))
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
          .withOrder(ProvidedOrder.asc(prop("a", "prop3")).fromRight)
          .withRHS(
            aPlan("Expand(All)")
              .withOrder(ProvidedOrder.asc(prop("a", "prop3")).fromLeft)
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

  test("Should support cartesian product with one ordered index and one other plan with no results") {
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a:Awesome), (n:NoResults) WHERE a.prop1 > 40 RETURN a, n").toList should be(empty)
  }

  test("Should keep order through correlated subquery with aggregation") {
    restartWithConfig(databaseConfig() ++  Map(
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(13),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(1024),
    ))
    val names = for (c <- 'a' to 'b'; i <- 0 until 100) yield {
      val name = f"$c$i%03d"
      createLabeledNode(Map("name" -> name, "age" -> i), "Person")
      name
    }

    graph.createIndex("Person", "name")
    val query =
      """MATCH (p:Person) WHERE p.name STARTS WITH ''
        |CALL {
        |  WITH p
        |  MATCH (p)-->(f)
        |  RETURN avg(f.age) AS a
        |}
        |RETURN p.name, a ORDER BY p.name
        |""".stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))

    val expected = names.map(name => Map("p.name" -> name, "a" -> null))
    result.toList should equal(expected)
  }


  // Some nodes which are suitable for CONTAINS and ENDS WITH testing
  private def createStringyNodes(tx: InternalTransaction) =
    tx.execute(
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

  private def createMoreNodes(tx: InternalTransaction) = {
    createSomeNodes(tx)
    tx.execute(
      """
        |CREATE (:Awesome {prop1: 40, prop2: 3, prop5: 'a'})
        |CREATE (:Awesome {prop1: 40, prop2: 1, prop5: 'b'})
        |CREATE (:Awesome {prop1: 40, prop2: 0, prop5: 'c'})
        |CREATE (:Awesome {prop1: 44, prop2: 4, prop5: 'd'})
        |CREATE (:Awesome {prop1: 44, prop2: 1, prop5: 'e'})
        |CREATE (:Awesome {prop1: 44, prop2: 1, prop5: 'f'})
        |CREATE (:Awesome {prop1: 44, prop2: 1, prop5: 'g'})
      """.stripMargin)
  }

  // Nodes for composite index independent of already existing indexes
  private def createNodesForComposite() =
    graph.withTx( tx => tx.execute(
      """
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'a', prop4: true, prop5: 3.14})
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'c', prop5: 2.72})
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'b', prop4: true, prop5: 1.67})
        |CREATE (:Label {prop1: 40, prop2: 5, prop3: 'b', prop4: false})
        |CREATE (:Label {prop1: 41, prop2: 2, prop3: 'b', prop5: 2.5})
        |CREATE (:Label {prop1: 41, prop2: 2, prop3: 'd', prop5: 2.5})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'a', prop4: true})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'c', prop4: true, prop5: 2.72})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'c', prop4: false, prop5: 3.14})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'c', prop5: 3.14})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'b', prop4: false})
        |CREATE (:Label {prop1: 41, prop2: 4, prop3: 'd', prop5: 1.67})
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
      """.stripMargin))

  // Used for named index tests, independent of already existing indexes
  // Invoked once before the Tx and once in the same Tx
  def createNodesForNamedIndex(tx: InternalTransaction): Unit = {
    tx.execute(
      """
      CREATE (:Label {prop: 40})-[:R]->()
      CREATE (:Label {prop: 41})-[:R]->()
      CREATE (:Label {prop: 42})-[:R]->()
      CREATE (:Label {prop: 43})-[:R]->()
      CREATE (:Label {prop: 44})-[:R]->()
      """)
  }
}
