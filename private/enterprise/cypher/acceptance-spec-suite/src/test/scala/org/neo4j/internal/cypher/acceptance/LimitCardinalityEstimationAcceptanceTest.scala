/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class LimitCardinalityEstimationAcceptanceTest extends ExecutionEngineFunSuite with CreateTempFileTestSupport with CypherComparisonSupport {

  test("should estimate rows when parameterized") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN p LIMIT $limit",
      params = Map("limit" -> 10))
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit")
        .withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows when literal") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN p LIMIT 10")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(10)
  }

  test("should estimate rows when literal and query has parameters") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) WHERE 50 = $fifty RETURN p LIMIT 10",
      params = Map("fifty" -> 50))
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(10)
  }

  test("should estimate rows when independent expression without parameters") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) with 10 as x, p RETURN p LIMIT toInteger(ceil(cos(0))) + 4")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(5)
  }

  test("should estimate rows by default value when expression contains parameter") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) with 10 as x, p RETURN p LIMIT toInteger(sin($limit))",
      params = Map("limit" -> 1))
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows by default value when expression contains rand()") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    // NOTE: We cannot executeWith because of random result
    val result = executeSingle("EXPLAIN MATCH (p:Person) with 10 AS x, p RETURN p LIMIT toInteger(rand()*10)", Map.empty)
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows by default value when expression contains timestamp()") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val r1 = executeSingle("EXPLAIN MATCH (p:Person) with 10 AS x, p RETURN p LIMIT timestamp()")
    r1.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)

    val r2 = executeSingle("EXPLAIN CYPHER runtime=slotted MATCH (p:Person) with 10 AS x, p RETURN p LIMIT timestamp()")
    r2.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)

    val r3 = executeSingle("EXPLAIN CYPHER runtime=interpreted MATCH (p:Person) with 10 AS x, p RETURN p LIMIT timestamp()")
    r3.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows after DISTINCT") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN DISTINCT p LIMIT 17")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit")
        .withEstimatedRows(17)
  }

  test("should estimate rows after aggregation") {
    (0 until 25*25).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN p, count(*) LIMIT 17")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit")
        .withEstimatedRows(17)
  }

  test("should estimate rows for skip + limit") {
    val nodeCount = 100
    (0 until nodeCount).foreach(_ => createNode())

    final case class TestCase(query: String, skip: Int, limit: Int)
    val defaultLimit = PlannerDefaults.DEFAULT_LIMIT_ROW_COUNT
    val defaultSkip = PlannerDefaults.DEFAULT_SKIP_ROW_COUNT

    Seq(
      TestCase("MATCH (n) RETURN n SKIP 17 LIMIT 42", // SKIP auto-parameterized
        skip = 42, limit = 42 + defaultSkip),
      TestCase("MATCH (n) RETURN n SKIP 17 LIMIT 500", // SKIP auto-parameterized
        skip = nodeCount - defaultSkip, limit = nodeCount),
      TestCase("MATCH (n) RETURN n SKIP 333 LIMIT 444", // SKIP auto-parameterized
        skip = nodeCount - defaultSkip, limit = nodeCount),
      TestCase("MATCH (n) RETURN n SKIP 12345 LIMIT 17", // SKIP auto-parameterized
        skip = 17, limit = 17 + defaultSkip),
      TestCase("MATCH (n) RETURN n SKIP $skip LIMIT $limit",
        skip = defaultLimit, limit = defaultLimit + defaultSkip),
      TestCase("MATCH (n) RETURN n SKIP 17 LIMIT $limit",
        skip = defaultLimit, limit = defaultLimit + 17),
      TestCase("MATCH (n) RETURN n, $prop SKIP 42 LIMIT 17",
        skip = 17, limit = 17 + 42),
      TestCase("MATCH (n) RETURN n, $prop SKIP 444 LIMIT 333",
        skip = 0, limit = nodeCount),
      TestCase("MATCH (n) RETURN n, $prop SKIP 0 LIMIT 17",
        skip = 17, limit = 17),
      TestCase("MATCH (n) RETURN n, $prop SKIP 0 LIMIT 0",
        skip = 0, limit = 0),
    ) foreach { t =>
      withClue(t) {
        val result = executeSingle(s"EXPLAIN ${t.query}", Map("prop" -> "hack", "skip" -> 17, "limit" -> 23))
        result.executionPlanDescription() should {
          includeSomewhere.aPlan("Skip").withEstimatedRows(t.skip) onTopOf
            aPlan("Limit").withEstimatedRows(t.limit)
        }
      }
    }

  }
}
