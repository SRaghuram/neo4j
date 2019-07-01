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
    grantTraverse().role("reader").node("*").map,
    grantTraverse().role("reader").relationship("*").map,
    grantRead().role("reader").node("*").map,
    grantRead().role("reader").relationship("*").map,

    grantTraverse().role("editor").node("*").map,
    grantTraverse().role("editor").relationship("*").map,
    grantRead().role("editor").node("*").map,
    grantRead().role("editor").relationship("*").map,
    grantWrite().role("editor").node("*").map,
    grantWrite().role("editor").relationship("*").map,

    grantTraverse().role("publisher").node("*").map,
    grantTraverse().role("publisher").relationship("*").map,
    grantRead().role("publisher").node("*").map,
    grantRead().role("publisher").relationship("*").map,
    grantWrite().role("publisher").node("*").map,
    grantWrite().role("publisher").relationship("*").map,
    grantToken().role("publisher").map,

    grantTraverse().role("architect").node("*").map,
    grantTraverse().role("architect").relationship("*").map,
    grantRead().role("architect").node("*").map,
    grantRead().role("architect").relationship("*").map,
    grantWrite().role("architect").node("*").map,
    grantWrite().role("architect").relationship("*").map,
    grantToken().role("architect").map,
    grantSchema().role("architect").map,

    grantTraverse().role("admin").node("*").map,
    grantTraverse().role("admin").relationship("*").map,
    grantRead().role("admin").node("*").map,
    grantRead().role("admin").relationship("*").map,
    grantWrite().role("admin").node("*").map,
    grantWrite().role("admin").relationship("*").map,
    grantToken().role("admin").map,
    grantSchema().role("admin").map,
    grantSystem().role("admin").map,
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

    def user(user: String) = PrivilegeMapBuilder(map + ("user" -> user))

    def property(property: String) = PrivilegeMapBuilder(map + ("resource" -> s"property($property)"))
  }

  def grantTraverse(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "graph")).node("*").action("find")
  def grantRead(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "all_properties")).node("*").action("read")
  def grantWrite(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "all_properties")).node("*").action("write")

  def grantToken(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "token")).action("write").node("*")
  def grantSchema(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "schema")).action("write").node("*")
  def grantSystem(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "system")).action("write").node("*")

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
