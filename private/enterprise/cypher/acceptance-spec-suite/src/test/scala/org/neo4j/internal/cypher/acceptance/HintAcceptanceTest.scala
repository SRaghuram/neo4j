/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.LocalDate

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Values

class HintAcceptanceTest
  extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should use a simple hint") {
    val query = "MATCH (a)--(b)--(c) USING JOIN ON b RETURN a,b,c"
    executeWith(Configs.All, query, planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeHashJoin")))
  }

  test("should not plan multiple joins for one hint - left outer join") {
    val a = createLabeledNode(Map[String, Any]("name" -> "a"), "A")
    for(i <- 0 until 10) {
      val b = createLabeledNode(Map[String, Any]("name" -> s"${i}b"), "B")
      relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion( p => {
        p should includeSomewhere.aPlan("NodeLeftOuterHashJoin")
        p should not(includeSomewhere.aPlan("NodeHashJoin"))
      }))
  }

  test("should not plan multiple joins for one hint - right outer join") {
    val b = createLabeledNode(Map[String, Any]("name" -> "b"), "B")
    for(i <- 0 until 10) {
      val a = createLabeledNode(Map[String, Any]("name" -> s"${i}a"), "A")
      if(i == 0) relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy = ComparePlansWithAssertion( p => {
      p should includeSomewhere.aPlan("NodeRightOuterHashJoin")
      p should not(includeSomewhere.aPlan("NodeHashJoin"))
    }))
  }

  test("should solve join hint on 1 variable with join on more, if possible") {
    val query =
      """MATCH (pA:Person),(pB:Person) WITH pA, pB
        |
        |OPTIONAL MATCH
        |  (pA)<-[:HAS_CREATOR]-(pB)
        |USING JOIN ON pB
        |RETURN *""".stripMargin

    executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion( p => {
        p should includeSomewhere.aPlan("NodeRightOuterHashJoin")
      }))
  }

  test("should solve join hints when leaves have extra variables") {
    val rel = relate(createNode(), createNode(), "p" -> 1)

    val query =
      s"""
         |    WITH 1 as nbr
         |    MATCH (n)-[r]->(p)
         |    USING JOIN ON p
         |    WHERE r.p = nbr
         |    RETURN r
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(_  should includeSomewhere.aPlan("NodeHashJoin")))

    result.toList should be(List(Map("r" -> rel)))
  }

  test("should do index seek instead of index scan with explicit index seek hint") {
    graph.createIndex("A", "prop")
    graph.createIndex("B", "prop")

    createLabeledNode(Map("prop" -> 42), "A")
    createLabeledNode(Map("prop" -> 1337), "B")

    // At the time of writing this test fails with generic index hints:
    // USING INDEX a:A(prop)
    // USING INDEX b:B(prop)
    val query = """EXPLAIN
                  |LOAD CSV WITH HEADERS FROM 'file:///dummy.csv' AS row
                  |MATCH (a:A), (b:B)
                  |USING INDEX SEEK a:A(prop)
                  |USING INDEX SEEK b:B(prop)
                  |WHERE a.prop = row.propA AND b.prop = row.propB
                  |RETURN a.prop, b.prop
                """.stripMargin

    executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(p => {
        p should includeSomewhere.nTimes(2, aPlan("NodeIndexSeek"))
      }))
  }

  test("should accept hint on spatial index with distance function") {
    // Given
    graph.createIndex("Business", "location")
    graph.createIndex("Review", "date")

    val business = createLabeledNode(Map("location" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -111.977, 33.3288)), "Business")
    val review = createLabeledNode(Map("date" -> LocalDate.parse("2017-03-01")), "Review")
    relate(review, business, "REVIEWS")

    // When
    val query =
      """MATCH (b:Business)<-[:REVIEWS]-(r:Review)
        |USING INDEX b:Business(location)
        |USING JOIN ON r //to avoid flaky plans
        |WHERE distance(b.location, point({latitude: 33.3288, longitude: -111.977})) < 6500
        |AND date("2017-01-01") <= r.date <= date("2018-01-01")
        |RETURN COUNT(*)""".stripMargin

    val result = executeWith(Configs.UDF, query)

    // Then
    result.toList should be(List(Map("COUNT(*)" -> 1)))

  }
}
