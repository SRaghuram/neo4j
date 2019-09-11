/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE
import java.util
import java.util.Collections

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.{ExecutionEngineFunSuite, ExecutionEngineHelper}
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.server.security.auth.SecurityTestUtils

import scala.collection.Map

abstract class AdministrationCommandAcceptanceTestBase extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {
  val neo4jUser: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN))
  val neo4jUserActive: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN), passwordChangeRequired = false)

  val defaultRolesWithUsers: Set[Map[String, Any]] = Set(
    Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true, "member" -> "neo4j"),
    Map("role" -> PredefinedRoles.ARCHITECT, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.PUBLISHER, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.EDITOR, "isBuiltIn" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.READER, "isBuiltIn" -> true, "member" -> null)
  )

  lazy val defaultRolePrivileges: Set[Map[String, AnyRef]] = Set(
    traverse().role("reader").node("*").map,
    traverse().role("reader").relationship("*").map,
    read().role("reader").node("*").map,
    read().role("reader").relationship("*").map,

    traverse().role("editor").node("*").map,
    traverse().role("editor").relationship("*").map,
    read().role("editor").node("*").map,
    read().role("editor").relationship("*").map,
    write().role("editor").node("*").map,
    write().role("editor").relationship("*").map,

    traverse().role("publisher").node("*").map,
    traverse().role("publisher").relationship("*").map,
    read().role("publisher").node("*").map,
    read().role("publisher").relationship("*").map,
    write().role("publisher").node("*").map,
    write().role("publisher").relationship("*").map,
    grantToken().role("publisher").map,

    traverse().role("architect").node("*").map,
    traverse().role("architect").relationship("*").map,
    read().role("architect").node("*").map,
    read().role("architect").relationship("*").map,
    write().role("architect").node("*").map,
    write().role("architect").relationship("*").map,
    grantToken().role("architect").map,
    grantSchema().role("architect").map,

    traverse().role("admin").node("*").map,
    traverse().role("admin").relationship("*").map,
    read().role("admin").node("*").map,
    read().role("admin").relationship("*").map,
    write().role("admin").node("*").map,
    write().role("admin").relationship("*").map,
    grantToken().role("admin").map,
    grantSchema().role("admin").map,
    grantSystem().role("admin").map,
  )

  def authManager: CommercialAuthManager = graph.getDependencyResolver.resolveDependency(classOf[CommercialAuthManager])

  override def databaseConfig(): Map[Setting[_], Object] = Map(GraphDatabaseSettings.auth_enabled -> TRUE)

  def selectDatabase(name: String): Unit = {
    graphOps = managementService.database(name)
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }

  def user(username: String, roles: Seq[String] = Seq.empty, suspended: Boolean = false, passwordChangeRequired: Boolean = true): Map[String, Any] = {
    Map("user" -> username, "roles" -> roles, "suspended" -> suspended, "passwordChangeRequired" -> passwordChangeRequired)
  }

  def setupUserWithCustomRole(username: String = "joe", password: String = "soap", rolename: String = "custom"): Unit = {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    execute(s"CREATE ROLE $rolename")
    execute(s"GRANT ROLE $rolename TO $username")
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

  def baseMap(grant: String = "GRANTED"): Map[String, String] = Map("grant" -> grant, "graph" -> "*")

  def traverse(grant: String = "GRANTED"): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "graph")).action("traverse")
  def read(grant: String = "GRANTED"): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "all_properties")).action("read")
  def write(grant: String = "GRANTED"): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "all_properties")).action("write")

  def grantToken(): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap() + ("resource" -> "token")).action("write").node("*")
  def grantSchema(): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap() + ("resource" -> "schema")).action("write").node("*")
  def grantSystem(): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap() + ("resource" -> "system")).action("write").node("*")

  type builderType = (PrivilegeMapBuilder, String) => PrivilegeMapBuilder
  def addNode(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.node(name)
  def addRel(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.relationship(name)

  def testUserLogin(username: String, password: String, expected: AuthenticationResult): Unit = {
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val result = login.subject().getAuthenticationResult
    result should be(expected)
  }

  def assertQueriesAndSubQueryCounts(queriesAndSubqueryCounts: Seq[(String, Int)]) {
    for (qc <- queriesAndSubqueryCounts) qc match {
      case (query, subqueryCount) =>
        val statistics = execute(query).queryStatistics()
        withClue(s"'$query': ") {
          statistics.containsUpdates should be(false)
          statistics.containsSystemUpdates should be(true)
          statistics.systemUpdates should be(subqueryCount)
        }
    }
  }

  def executeOnDefault(username: String, password: String, query: String,
                       params: util.Map[String, Object] = Collections.emptyMap(),
                       resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {},
                       executeBefore: InternalTransaction => Unit = _ => ()): Int = {
    executeOn(GraphDatabaseSettings.DEFAULT_DATABASE_NAME, username, password, query, params, resultHandler, executeBefore)
  }

  def executeOnSystem(username: String, password: String, query: String,
                      params: util.Map[String, Object] = Collections.emptyMap(),
                      resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {},
                      executeBefore: InternalTransaction => Unit = _ => ()): Int = {
    executeOn(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, username, password, query, params, resultHandler, executeBefore)
  }

  def executeOn(database: String, username: String, password: String, query: String,
                params: util.Map[String, Object] = Collections.emptyMap(),
                resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {},
                executeBefore: InternalTransaction => Unit = _ => ()): Int = {
    selectDatabase(database)
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val tx = graph.beginTransaction(Transaction.Type.explicit, login)
    try {
      executeBefore(tx)
      var count = 0
      val result: Result = tx.execute(query, params)
      result.accept(row => {
        resultHandler(row, count)
        count = count + 1
        true
      })
      tx.commit()
      count
    } finally {
      tx.close()
    }
  }

}
