/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.runtime.{ExplainMode, NormalMode}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class ExplainAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("normal query is marked as such") {
    createNode()
    val result = executeWith(Configs.All, "match (n) return n")

    result.executionMode should equal(NormalMode)
    result shouldNot be(empty)
  }

  test("explain query is marked as such") {
    createNode()
    val result = executeWith(Configs.All, "explain match (n) return n")

    result.executionMode should equal(ExplainMode)
    result should be(empty)
  }

  test("EXPLAIN for Cypher 3.1") {
    val result = executeOfficial("explain match (n) return n")
    result.resultAsString()
    result.getExecutionPlanDescription.toString should include("Estimated Rows")
  }

  test("should handle query with nested expression") {
    val query = """EXPLAIN
                  |WITH
                  |   ['Herfstvakantie Noord'] AS periodName
                  |MATCH (perStart:Day)<-[:STARTS]-(per:Period)-[:ENDS]->(perEnd:Day) WHERE per.naam=periodName
                  |WITH perStart,perEnd
                  |
                  |MATCH perDays=shortestPath((perStart)-[:NEXT*]->(perEnd))
                  |UNWIND nodes(perDays) as perDay
                  |WITH perDay ORDER by perDay.day
                  |
                  |MATCH (bknStart:Day)-[:NEXT*0..]->(perDay)
                  |WHERE (bknStart)<-[:FROM_DATE]-(:Boeking)
                  |WITH distinct bknStart, collect(distinct perDay) as perDays
                  |
                  |MATCH (bknStart)<-[:FROM_DATE]-(bkn:Boeking)-[:TO_DATE]->(bknEnd)
                  |WITH bknEnd, collect(bkn) as bookings, perDays
                  |WHERE any(perDay IN perDays WHERE perDays = bknEnd OR exists((perDay)-[:NEXT*]->(bknEnd)))
                  |
                  |RETURN count(*), count(distinct bknEnd), avg(size(bookings)),avg(size(perDays));""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val plan = result.executionPlanDescription().toString

    plan.toString should include("NestedPlanExpression(VarExpand-Argument)")
  }
}
