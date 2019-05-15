/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ReplanningAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_min_replan_interval -> "100ms",
    GraphDatabaseSettings.query_statistics_divergence_threshold -> "0.1"
  )

  test("should replan query from AllNodeScan to NodeById as the graph grows") {
    val params = Map("rows" -> List(
                       Map("startNodeId" -> 374337L,
                           "relRef" -> -200864L,
                           "endNodeId" -> 540311L,
                           "props" -> Map("timestamp" -> 1552893253242L)
                       )
                     ))

    val query =
      """
        |EXPLAIN
        |UNWIND {rows} as row
        |MATCH (startNode) WHERE ID(startNode) = row.startNodeId
        |MATCH (endNode) WHERE ID(endNode) = row.endNodeId
        |CREATE (startNode)-[rel:R]->(endNode) SET rel += row.props
        |RETURN rel
      """.stripMargin

    // given
    val planForEmptyGraph = executeSingle(query, params).executionPlanDescription()
    planForEmptyGraph should includeSomewhere.aPlan("AllNodesScan")
    planForEmptyGraph should not(includeSomewhere.aPlan("NodeByIdSeek"))

    // when
    graph.inTx {
      (0 until 1000).map(_ => createNode())
    }
    Thread.sleep(200)

    // then
    val planForPopulatedGraph = executeSingle(query, params).executionPlanDescription()
    planForPopulatedGraph should includeSomewhere.aPlan("NodeByIdSeek")
    planForPopulatedGraph should not(includeSomewhere.aPlan("AllNodesScan"))
  }
}
