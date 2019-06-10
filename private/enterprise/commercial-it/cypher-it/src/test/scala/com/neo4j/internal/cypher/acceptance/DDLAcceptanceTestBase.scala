/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.Optional

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.{ExecutionEngineFunSuite, ExecutionEngineHelper}
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.server.security.auth.SecurityTestUtils

import scala.collection.Map

abstract class DDLAcceptanceTestBase extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {
  val grantMap = Map("grant" -> "GRANTED", "database" -> "*", "label" -> "*")

  val defaultRolesWithUsers: Set[Map[String, Any]] = Set(
    Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true, "member" -> "neo4j"),
    Map("role" -> PredefinedRoles.ARCHITECT, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.PUBLISHER, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.EDITOR, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.READER, "isBuiltIn" -> true, "member" -> null)
  )

  lazy val defaultRolePrivileges = Set(
    grantGraph().role("reader").action("find").map,
    grantGraph().role("reader").action("read").map,

    grantGraph().role("editor").action("find").map,
    grantGraph().role("editor").action("read").map,
    grantGraph().role("editor").action("write").map,

    grantGraph().role("publisher").action("find").map,
    grantGraph().role("publisher").action("read").map,
    grantGraph().role("publisher").action("write").map,
    grantToken().role("publisher").action("write").map,

    grantGraph().role("architect").action("find").map,
    grantGraph().role("architect").action("read").map,
    grantGraph().role("architect").action("write").map,
    grantToken().role("architect").action("write").map,
    grantSchema().role("architect").action("write").map,

    grantGraph().role("admin").action("find").map,
    grantGraph().role("admin").action("read").map,
    grantGraph().role("admin").action("write").map,
    grantSystem().role("admin").action("write").map,
    grantToken().role("admin").action("write").map,
    grantSchema().role("admin").action("write").map,
  )

  def authManager: CommercialAuthManager = graph.getDependencyResolver.resolveDependency(classOf[CommercialAuthManager])
  def databaseManager: DatabaseManager[DatabaseContext] = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])

  override def databaseConfig(): Map[Setting[_], String] = Map(GraphDatabaseSettings.auth_enabled -> "true")

  def selectDatabase(name: String): Unit = {
    val maybeCtx: Optional[DatabaseContext] = databaseManager.getDatabaseContext(new TestDatabaseIdRepository().get(name))
    val dbCtx: DatabaseContext = maybeCtx.orElseGet(() => throw new RuntimeException(s"No such database: $name"))
    graphOps = dbCtx.databaseFacade()
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }

  case class PrivilegeMapBuilder(map: Map[String, AnyRef]) {
    def action(action: String) = PrivilegeMapBuilder(map + ("action" -> action))

    def role(role: String) = PrivilegeMapBuilder(map + ("role" -> role))

    def label(label: String) = PrivilegeMapBuilder(map + ("label" -> label))

    def database(database: String) = PrivilegeMapBuilder(map + ("database" -> database))

    def resource(resource: String) = PrivilegeMapBuilder(map + ("resource" -> resource))

    def user(user: String) = PrivilegeMapBuilder(map + ("user" -> user))

    def property(property: String) = PrivilegeMapBuilder(map + ("resource" -> s"property($property)"))
  }
  def grantTraverse(): PrivilegeMapBuilder = grantGraph().action("find")
  def grantRead(): PrivilegeMapBuilder = grantGraph().action("read").resource("all_properties")
  def grantGraph(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "graph"))
  def grantSchema(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "schema"))
  def grantToken(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "token"))
  def grantSystem(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "system"))

  def executeOnDefault(username: String, password: String, query: String, resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    executeOn(GraphDatabaseSettings.DEFAULT_DATABASE_NAME, username, password, query, resultHandler)
  }

  def executeOnSystem(username: String, password: String, query: String, resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    executeOn(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, username, password, query, resultHandler)
  }

  private def executeOn(database: String, username: String, password: String, query: String, resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    selectDatabase(database)
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val tx = graph.beginTransaction(Transaction.Type.explicit, login)
    try {
      var count = 0
      val result: Result = new RichGraphDatabaseQueryService(graph).execute(query)
      result.accept(row => {
        resultHandler(row, count)
        count = count + 1
        true
      })
      tx.success()
      count
    } finally {
      tx.close()
    }
  }

}
