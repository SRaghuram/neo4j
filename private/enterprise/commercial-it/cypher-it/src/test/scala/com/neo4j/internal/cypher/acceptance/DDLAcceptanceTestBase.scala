/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util
import java.util.{Collections, Optional}

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
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.server.security.auth.SecurityTestUtils

import scala.collection.Map

abstract class DDLAcceptanceTestBase extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {
  val neo4jUser: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN))
  val neo4jUserActive: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN), passwordChangeRequired = false)

  val grantMap: Map[String, String] = Map("grant" -> "GRANTED", "graph" -> "*")

  val defaultRolesWithUsers: Set[Map[String, Any]] = Set(
    Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true, "member" -> "neo4j"),
    Map("role" -> PredefinedRoles.ARCHITECT, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.PUBLISHER, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.EDITOR, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.READER, "isBuiltIn" -> true, "member" -> null)
  )

  lazy val defaultRolePrivileges: Set[Map[String, AnyRef]] = Set(
    grantGraph().role("reader").action("find").node("*").map,
    grantGraph().role("reader").action("read").node("*").map,
    grantGraph().role("reader").action("find").relationship("*").map,
    grantGraph().role("reader").action("read").relationship("*").map,

    grantGraph().role("editor").action("find").node("*").map,
    grantGraph().role("editor").action("read").node("*").map,
    grantGraph().role("editor").action("write").node("*").map,
    grantGraph().role("editor").action("find").relationship("*").map,
    grantGraph().role("editor").action("read").relationship("*").map,

    grantGraph().role("publisher").action("find").node("*").map,
    grantGraph().role("publisher").action("read").node("*").map,
    grantGraph().role("publisher").action("write").node("*").map,
    grantToken().role("publisher").action("write").node("*").map,
    grantGraph().role("publisher").action("find").relationship("*").map,
    grantGraph().role("publisher").action("read").relationship("*").map,

    grantGraph().role("architect").action("find").node("*").map,
    grantGraph().role("architect").action("read").node("*").map,
    grantGraph().role("architect").action("write").node("*").map,
    grantToken().role("architect").action("write").node("*").map,
    grantSchema().role("architect").action("write").node("*").map,
    grantGraph().role("architect").action("find").relationship("*").map,
    grantGraph().role("architect").action("read").relationship("*").map,

    grantGraph().role("admin").action("find").node("*").map,
    grantGraph().role("admin").action("read").node("*").map,
    grantGraph().role("admin").action("write").node("*").map,
    grantSystem().role("admin").action("write").node("*").map,
    grantToken().role("admin").action("write").node("*").map,
    grantSchema().role("admin").action("write").node("*").map,
    grantGraph().role("admin").action("find").relationship("*").map,
    grantGraph().role("admin").action("read").relationship("*").map,
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

  def user(username: String, roles: Seq[String] = Seq.empty, suspended: Boolean = false, passwordChangeRequired: Boolean = true): Map[String, Any] = {
    Map("user" -> username, "roles" -> roles, "suspended" -> suspended, "passwordChangeRequired" -> passwordChangeRequired)
  }

  case class PrivilegeMapBuilder(map: Map[String, AnyRef]) {
    def action(action: String) = PrivilegeMapBuilder(map + ("action" -> action))

    def role(role: String) = PrivilegeMapBuilder(map + ("role" -> role))

    def node(label: String) = PrivilegeMapBuilder(map + ("segment" -> s"NODE($label)"))

    def relationship(relType: String) = PrivilegeMapBuilder(map + ("segment" -> s"RELATIONSHIP($relType)"))

    def database(database: String) = PrivilegeMapBuilder(map + ("graph" -> database))

    def resource(resource: String) = PrivilegeMapBuilder(map + ("resource" -> resource))

    def user(user: String) = PrivilegeMapBuilder(map + ("user" -> user))

    def property(property: String) = PrivilegeMapBuilder(map + ("resource" -> s"property($property)"))
  }
  def grantTraverse(): PrivilegeMapBuilder = grantGraph().action("find")
  def grantRead(): PrivilegeMapBuilder = grantGraph().action("read").resource("all_properties")
  def grantWrite(): PrivilegeMapBuilder = grantGraph().action("write").resource("all_properties")
  def grantGraph(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "graph")).node("*")
  def grantSchema(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "schema")).node("*")
  def grantToken(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "token")).node("*")
  def grantSystem(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "system")).node("*")

  def testUserLogin(username: String, password: String, expected: AuthenticationResult): Unit = {
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val result = login.subject().getAuthenticationResult
    result should be(expected)
  }

  def executeOnDefault(username: String, password: String, query: String,
                       params: util.Map[String, Object] = Collections.emptyMap(),
                       resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    executeOn(GraphDatabaseSettings.DEFAULT_DATABASE_NAME, username, password, query, params, resultHandler)
  }

  def executeOnSystem(username: String, password: String, query: String,
                      params: util.Map[String, Object] = Collections.emptyMap(),
                      resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    executeOn(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, username, password, query, params, resultHandler)
  }

  def executeOn(database: String, username: String, password: String, query: String,
                        params: util.Map[String, Object] = Collections.emptyMap(),
                        resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    selectDatabase(database)
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val tx = graph.beginTransaction(Transaction.Type.explicit, login)
    try {
      var count = 0
      val result: Result = new RichGraphDatabaseQueryService(graph).execute(query, params)
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
