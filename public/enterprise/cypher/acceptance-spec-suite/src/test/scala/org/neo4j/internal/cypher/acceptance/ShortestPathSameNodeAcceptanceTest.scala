/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compatibility.ClosingExecutionResult
import org.neo4j.cypher.internal.{CompatibilityFactory, ExecutionEngine, RewindableExecutionResult}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, RunWithConfigTestSupport, ShortestPathCommonEndNodesForbiddenException}
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.logging.NullLogProvider

class ShortestPathSameNodeAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport with RunWithConfigTestSupport {

  def setupModel(db: GraphDatabaseCypherService) {
    db.inTx {
      val a = db.createNode()
      val b = db.createNode()
      val c = db.createNode()
      a.createRelationshipTo(b, RelationshipType.withName("KNOWS"))
      b.createRelationshipTo(c, RelationshipType.withName("KNOWS"))
    }
  }

  test("shortest paths with explicit same start and end nodes should throw exception by default") {
    setupModel(graph)
    val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
    an[ShortestPathCommonEndNodesForbiddenException] should be thrownBy executeWithAllPlanners(query)
  }

  test("shortest paths with explicit same start and end nodes should throw exception when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "true") { db =>
      setupModel(db)
      val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
      val error = intercept[ShortestPathCommonEndNodesForbiddenException](
        executeUsingCostPlannerOnly(db, query).toList
      ).getMessage should include("The shortest path algorithm does not work when the start and end nodes are the same")
    }
  }

  test("shortest paths with explicit same start and end nodes should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "false") { db =>
      setupModel(db)
      val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(0)
    }
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should throw exception by default") {
    setupModel(graph)
    val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
    an[ShortestPathCommonEndNodesForbiddenException] should be thrownBy executeWithAllPlanners(query)
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should throw exception when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "true") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
      val error = intercept[ShortestPathCommonEndNodesForbiddenException](
        executeUsingCostPlannerOnly(db, query).toList
      ).getMessage should include("The shortest path algorithm does not work when the start and end nodes are the same")
    }
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "false") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(6)
    }
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should not throw exception by default") {
    setupModel(graph)
    val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
    executeWithAllPlanners(query).toList.length should be(9)
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should throw exception even when when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "true") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(9)
    }
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "false") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(9)
    }
  }

  def executeUsingCostPlannerOnly(db: GraphDatabaseCypherService, query: String) = {
    val compatibilityFactory = db.getDependencyResolver.resolveDependency(classOf[CompatibilityFactory])
    new ExecutionEngine(db, NullLogProvider.getInstance(), compatibilityFactory).execute(s"CYPHER planner=COST $query", Map.empty[String, Any]) match {
      case e: ClosingExecutionResult => RewindableExecutionResult(e.inner)
    }
  }
}
