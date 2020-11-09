/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class QueryPlanCompatibilityTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should produce compatible plans for simple MATCH node query") {
    val query = "MATCH (n:Person) RETURN n"
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan)))
  }

  test("should produce compatible plans for simple MATCH relationship query") {
    val query = "MATCH (n:Person)-[r:KNOWS]->(m) RETURN r"
    executeWith(Configs.All, query)
  }

  test("should produce compatible plans with predicates") {
    val query =
      """
        |MATCH (n:Person) WHERE n.name STARTS WITH 'Joe' AND n.age >= 42
        |RETURN count(n)
      """.stripMargin
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan)))
  }

  test("should produce compatible plans with unwind") {
    val query =
      """
        |WITH 'Joe' as name
        |UNWIND [42,43,44] as age
        |MATCH (n:Person) WHERE n.name STARTS WITH name AND n.age >= age
        |RETURN count(n)
      """.stripMargin
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan)))
  }

  test("should produce compatible plans for complex query") {
    val query =
      """
        |WITH 'Joe' as name
        |UNWIND [42,43,44] as age
        |MATCH (n:Person) WHERE n.name STARTS WITH name AND n.age >= age
        |OPTIONAL MATCH (n)-[r:KNOWS]->(m) WHERE r.since IS NOT NULL
        |RETURN count(r)
      """.stripMargin
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan)))
  }

  private def assertSimilarPlans(plan: InternalPlanDescription, expected: InternalPlanDescription): Unit = {
    plan.flatten.map(simpleName).toString should equal(expected.flatten.map(simpleName).toString())
  }

  private def generateExpectedPlan(query: String): InternalPlanDescription = executeSingle(query, Map.empty).executionPlanDescription()

  private def simpleName(plan: InternalPlanDescription): String = plan.name.replace("SetNodeProperty", "SetProperty").toLowerCase
}
