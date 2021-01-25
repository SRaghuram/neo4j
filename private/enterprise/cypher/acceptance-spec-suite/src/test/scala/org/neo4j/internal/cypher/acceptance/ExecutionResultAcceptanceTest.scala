/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.graphdb.Result
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.kernel.impl.query.QuerySubscriber.DO_NOTHING_SUBSCRIBER
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

class ExecutionResultAcceptanceTest extends ExecutionEngineFunSuite{

  test("closing the result without exhausting it should not fail the transaction") {
    val query = "UNWIND [1, 2, 3] as x RETURN x"

    Configs.All.scenarios.map(s =>
      s"CYPHER ${s.preparserOptions} $query").foreach(q => {
      graph.withTx { tx =>
        val result = eengine.execute(q,
          EMPTY_MAP,
          graph.transactionalContext(tx, query = q -> Map.empty),
          profile = false,
          prePopulate = false,
          DO_NOTHING_SUBSCRIBER)
        result.cancel()
      }
    })
  }

  test("without PROFILE it shouldn't be needed to iterate over the results before calling getExecutionPlanDescription in interpreted runtime") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n"
    val runtime = CypherRuntimeOption.interpreted.name

    val description1 = executeQueryAndGetExecutionPlanDescription(query, runtime, iterateOverResult = false)
    val description2 = executeQueryAndGetExecutionPlanDescription(query, runtime, iterateOverResult = true)
    description1 should equal(description2)
  }

  test("without PROFILE it shouldn't be needed to iterate over the results before calling getExecutionPlanDescription in slotted runtime") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n"
    val runtime = CypherRuntimeOption.slotted.name

    val description1 = executeQueryAndGetExecutionPlanDescription(query, runtime, iterateOverResult = false)
    val description2 = executeQueryAndGetExecutionPlanDescription(query, runtime, iterateOverResult = true)
    description1 should equal(description2)
  }

  private def executeQueryAndGetExecutionPlanDescription(query: String, runtime: String, iterateOverResult: Boolean) = {
    val executedQuery = "CYPHER runtime = " + runtime + " " + query
    graph.withTx( tx => {
      val result: Result = tx.execute(executedQuery)
      if (iterateOverResult)
        result.hasNext should be (false) // don't really care for the assertion, just consume the results
      val description = result.getExecutionPlanDescription
      result.close()
      description
    })
  }

}
