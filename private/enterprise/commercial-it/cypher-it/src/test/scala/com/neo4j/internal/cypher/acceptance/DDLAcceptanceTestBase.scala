/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.Optional

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.{ExecutionEngineFunSuite, ExecutionEngineHelper}
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles

import scala.collection.Map

abstract class DDLAcceptanceTestBase extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {
  val defaultRolesWithUsers: Set[Map[String, Any]] = Set(
    Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"),
    Map("role" -> PredefinedRoles.ARCHITECT, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.PUBLISHER, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.EDITOR, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.READER, "is_built_in" -> true, "member" -> null)
  )

  def authManager: CommercialAuthManager = graph.getDependencyResolver.resolveDependency(classOf[CommercialAuthManager])
  def databaseManager: DatabaseManager[DatabaseContext] = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])

  override def databaseConfig(): Map[Setting[_], String] = Map(GraphDatabaseSettings.auth_enabled -> "true")

  def selectDatabase(name: String): Unit = {
    val maybeCtx: Optional[DatabaseContext] = databaseManager.getDatabaseContext(new DatabaseId(name))
    val dbCtx: DatabaseContext = maybeCtx.orElseGet(() => throw new RuntimeException(s"No such database: $name"))
    graphOps = dbCtx.databaseFacade()
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }
}
