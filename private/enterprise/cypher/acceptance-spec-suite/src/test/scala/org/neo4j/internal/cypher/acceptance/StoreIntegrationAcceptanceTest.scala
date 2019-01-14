/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}
import org.neo4j.internal.recordstorage.RecordStorageEngine

class StoreIntegrationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  // Not TCK material
  test("should not create labels id when trying to delete non-existing labels") {
    createNode()

    val result = executeWith(Configs.InterpretedAndSlotted, "MATCH (n) REMOVE n:BAR RETURN id(n) AS id")

    assertStats(result, labelsRemoved = 0)
    result.toList should equal(List(Map("id" -> 0)))

    graph.inTx {
      graph.getDependencyResolver.resolveDependency(classOf[RecordStorageEngine]).testAccessNeoStores().getLabelTokenStore.getHighId should equal(0)
    }
  }
}
