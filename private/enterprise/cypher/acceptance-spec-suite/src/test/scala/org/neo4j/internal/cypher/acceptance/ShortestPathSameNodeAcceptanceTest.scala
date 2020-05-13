/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Boolean.FALSE
import java.lang.Boolean.TRUE

import com.neo4j.cypher.RunWithConfigTestSupport
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.ExecutionEngineHelper
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.exceptions.ShortestPathCommonEndNodesForbiddenException
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.values.virtual.VirtualValues

class ShortestPathSameNodeAcceptanceTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport with CypherComparisonSupport {

  def setupModel(db: GraphDatabaseCypherService) {
    db.withTx { tx =>
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      a.createRelationshipTo(b, RelationshipType.withName("KNOWS"))
      b.createRelationshipTo(c, RelationshipType.withName("KNOWS"))
    }
  }

  test("shortest paths with explicit same start and end nodes should throw exception by default") {
    setupModel(graph)
    val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
    failWithError(Configs.ShortestPath, query, List("The shortest path algorithm does not work when the start and end nodes are the same."))
  }

  test("shortest paths with explicit same start and end nodes should throw exception when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> TRUE) { db =>
      setupModel(db)
      val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
      intercept[ShortestPathCommonEndNodesForbiddenException](
        executeUsingCostPlannerOnly(db, query).toList
      ).getMessage should include("The shortest path algorithm does not work when the start and end nodes are the same")
    }
  }

  test("shortest paths with explicit same start and end nodes should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> FALSE) { db =>
      setupModel(db)
      val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(0)
    }
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should throw exception by default") {
    setupModel(graph)
    val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
    failWithError(Configs.ShortestPath /\ Configs.CartesianProduct,
      query, List("The shortest path algorithm does not work when the start and end nodes are the same."))
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should throw exception when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> TRUE) { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
      intercept[ShortestPathCommonEndNodesForbiddenException](
        executeUsingCostPlannerOnly(db, query).toList
      ).getMessage should include("The shortest path algorithm does not work when the start and end nodes are the same")
    }
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> FALSE) { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(6)
    }
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should not throw exception by default") {
    setupModel(graph)
    val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
    executeWith(Configs.ShortestPath /\ Configs.CartesianProduct,
      query).toList.length should be(9)
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should throw exception even when when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> TRUE) { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(9)
    }
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> FALSE) { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(9)
    }
  }

  def executeUsingCostPlannerOnly(db: GraphDatabaseCypherService, query: String): RewindableExecutionResult = {
    db.withTx { tx =>
      val engine = ExecutionEngineHelper.createEngine(db)
      val subscriber = new RecordingQuerySubscriber
      val context = engine.queryService.transactionalContext(tx, query = query -> Map())
      val result = engine.execute(query,
        VirtualValues.EMPTY_MAP,
        context,
        profile = false,
        prePopulate = false,
        subscriber)

      RewindableExecutionResult(result,
        new TransactionBoundQueryContext(TransactionalContextWrapper(context), new ResourceManager)(mock[IndexSearchMonitor]),
        subscriber)
    }
  }
}
