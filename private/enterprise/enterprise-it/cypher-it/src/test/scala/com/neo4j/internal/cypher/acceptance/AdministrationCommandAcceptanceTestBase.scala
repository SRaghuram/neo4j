/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.io.File
import java.lang.Boolean.TRUE
import java.nio.file.Files
import java.util
import java.util.Collections

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import com.neo4j.dbms.EnterpriseSystemGraphInitializer
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphInitializer
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl
import org.neo4j.cypher.internal.security.SecureHasher
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.InMemoryUserRepository
import org.neo4j.server.security.auth.SecurityTestUtils

abstract class AdministrationCommandAcceptanceTestBase extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {
  val GRANTED: String = "GRANTED"
  val DENIED: String = "DENIED"
  val DEFAULT: String = "DEFAULT"

  val neo4jUser: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN))
  val neo4jUserActive: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN), passwordChangeRequired = false)
  val onlineStatus: String = DatabaseStatus.Online.stringValue()
  val offlineStatus: String = DatabaseStatus.Offline.stringValue()

  val defaultRoles: Set[Map[String, Any]] = Set(
    role(PUBLIC).builtIn().map,
    role(PredefinedRoles.ADMIN).builtIn().map,
    role(PredefinedRoles.ARCHITECT).builtIn().map,
    role(PredefinedRoles.PUBLISHER).builtIn().map,
    role(PredefinedRoles.EDITOR).builtIn().map,
    role(PredefinedRoles.READER).builtIn().map
  )

  val defaultRolesWithUsers: Set[Map[String, Any]] = Set(
    role(PUBLIC).builtIn().member("neo4j").map,
    role(PredefinedRoles.ADMIN).builtIn().member("neo4j").map,
    role(PredefinedRoles.ARCHITECT).builtIn().noMember().map,
    role(PredefinedRoles.PUBLISHER).builtIn().noMember().map,
    role(PredefinedRoles.EDITOR).builtIn().noMember().map,
    role(PredefinedRoles.READER).builtIn().noMember().map
  )

  lazy val defaultRolePrivileges: Set[Map[String, AnyRef]] = Set(
    access().database("DEFAULT").role("PUBLIC").map,

    access().role("reader").map,
    traverse().role("reader").node("*").map,
    traverse().role("reader").relationship("*").map,
    read().role("reader").node("*").map,
    read().role("reader").relationship("*").map,

    access().role("editor").map,
    traverse().role("editor").node("*").map,
    traverse().role("editor").relationship("*").map,
    read().role("editor").node("*").map,
    read().role("editor").relationship("*").map,
    write().role("editor").node("*").map,
    write().role("editor").relationship("*").map,

    access().role("publisher").map,
    traverse().role("publisher").node("*").map,
    traverse().role("publisher").relationship("*").map,
    read().role("publisher").node("*").map,
    read().role("publisher").relationship("*").map,
    write().role("publisher").node("*").map,
    write().role("publisher").relationship("*").map,
    grantToken().role("publisher").map,

    access().role("architect").map,
    traverse().role("architect").node("*").map,
    traverse().role("architect").relationship("*").map,
    read().role("architect").node("*").map,
    read().role("architect").relationship("*").map,
    write().role("architect").node("*").map,
    write().role("architect").relationship("*").map,
    grantToken().role("architect").map,
    grantSchema().role("architect").map,

    access().role("admin").map,
    traverse().role("admin").node("*").map,
    traverse().role("admin").relationship("*").map,
    read().role("admin").node("*").map,
    read().role("admin").relationship("*").map,
    write().role("admin").node("*").map,
    write().role("admin").relationship("*").map,
    grantToken().role("admin").map,
    grantSchema().role("admin").map,
    grantAdmin().role("admin").map,
  )

  def defaultRolePrivilegesFor(role: String): Set[Map[String, AnyRef]] = defaultRolePrivileges.filter(m => m("role") == role)

  def defaultRolePrivilegesFor(role: String, replace: String): Set[Map[String, AnyRef]] = {
    defaultRolePrivileges.foldLeft(Set.empty[Map[String, AnyRef]]) {
      case (acc, row) if row("role") == role =>
        acc + row.map {
          case (k, _) if k == "role" => (k, replace)
          case (k, v) => (k, v)
        }
      case (acc, _) => acc
    }
  }

  def authManager: EnterpriseAuthManager = graph.getDependencyResolver.resolveDependency(classOf[EnterpriseAuthManager])

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.auth_enabled -> TRUE)

  def user(username: String, roles: Seq[String] = Seq.empty, suspended: Boolean = false, passwordChangeRequired: Boolean = true): Map[String, Any] = {
    val rolesWithPublic = roles.sorted :+ PUBLIC
    Map("user" -> username, "roles" -> rolesWithPublic, "suspended" -> suspended, "passwordChangeRequired" -> passwordChangeRequired)
  }

  def db(name: String, status: String = onlineStatus, default: Boolean = false): Map[String, Any] =
    Map("name" -> name,
      "address" -> "localhost:7687",
      "role" -> "standalone",
      "requestedStatus" -> status,
      "currentStatus" -> status,
      "error" -> "",
      "default" -> default)

  def defaultDb(name: String = DEFAULT_DATABASE_NAME, status: String = onlineStatus): Map[String, String] =
    Map("name" -> name,
      "address" -> "localhost:7687",
      "role" -> "standalone",
      "requestedStatus" -> status,
      "currentStatus" -> status,
      "error" -> "")

  def setupUserWithCustomRole(username: String = "joe", password: String = "soap", rolename: String = "custom", access: Boolean = true): Unit = {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    execute(s"CREATE ROLE $rolename")
    execute(s"GRANT ROLE $rolename TO $username")
    if (access) execute(s"GRANT ACCESS ON DATABASE * TO $rolename")
  }

  case class RoleMapBuilder(map: Map[String, Any]) {
    def member(user: String) = RoleMapBuilder(map + ("member" -> user))

    def noMember() = RoleMapBuilder(map + ("member" -> null))

    def builtIn() = RoleMapBuilder(map + ("isBuiltIn" -> true))
  }

  def role(roleName: String): RoleMapBuilder = RoleMapBuilder(Map("role" -> roleName, "isBuiltIn" -> false))

  def publicRole(users: String*): Set[Map[String, Any]] =
    users.map(u => role(PUBLIC).builtIn().member(u).map).toSet

  case class PrivilegeMapBuilder(map: Map[String, AnyRef]) {
    def action(action: String) = PrivilegeMapBuilder(map + ("action" -> action))

    def role(role: String) = PrivilegeMapBuilder(map + ("role" -> role))

    def node(label: String) = PrivilegeMapBuilder(map + ("segment" -> s"NODE($label)"))

    def relationship(relType: String) = PrivilegeMapBuilder(map + ("segment" -> s"RELATIONSHIP($relType)"))

    def database(database: String) = PrivilegeMapBuilder(map + ("graph" -> database))

    def user(user: String) = PrivilegeMapBuilder(map + ("user" -> user))

    def property(property: String) = PrivilegeMapBuilder(map + ("resource" -> s"property($property)"))
  }

  def baseMap(grant: String = GRANTED): Map[String, String] = Map("access" -> grant, "graph" -> "*", "segment" -> "database")

  def adminAction(action: String, grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ( "resource" -> "database" )).action(action)

  def startDatabase(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("start_database")
  def stopDatabase(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("stop_database")

  def createIndex(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("create_index")
  def dropIndex(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("drop_index")
  def createConstraint(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("create_constraint")
  def dropConstraint(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("drop_constraint")

  def createNodeLabel(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("create_label")
  def createRelationshipType(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("create_reltype")
  def createPropertyKey(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("create_propertykey")

  def access(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database")).action("access")
  def traverse(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "graph")).action("traverse")
  def read(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "all_properties")).action("read")
  def write(grant: String = GRANTED): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "all_properties")).action("write")

  def grantToken(): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap() + ("resource" -> "database")).action("token")
  def grantSchema(): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap() + ("resource" -> "database")).action("schema")
  def grantAdmin(): PrivilegeMapBuilder = PrivilegeMapBuilder(baseMap() + ("resource" -> "database")).action("admin")

  def showTransaction(username: String, grant: String = GRANTED): PrivilegeMapBuilder =
    PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database", "segment" -> s"USER($username)")).action("show_transaction")
  def terminateTransaction(username: String, grant: String = GRANTED): PrivilegeMapBuilder =
    PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database", "segment" -> s"USER($username)")).action("terminate_transaction")
  def transaction(username: String, grant: String = GRANTED): PrivilegeMapBuilder =
    PrivilegeMapBuilder(baseMap(grant) + ("resource" -> "database", "segment" -> s"USER($username)")).action("transaction_management")

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
                       executeBefore: InternalTransaction => Unit = _ => (),
                       requiredOperator: Option[String] = None): Int = {
    executeOn(GraphDatabaseSettings.DEFAULT_DATABASE_NAME, username, password, query, params, resultHandler, executeBefore, requiredOperator)
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
                executeBefore: InternalTransaction => Unit = _ => (),
                requiredOperator: Option[String] = None): Int = {
    selectDatabase(database)
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val tx = graph.beginTransaction(Type.EXPLICIT, login)
    try {
      executeBefore(tx)
      var count = 0
      val result: Result = tx.execute(query, params)
      result.accept(row => {
        resultHandler(row, count)
        count = count + 1
        true
      })
      requiredOperator.foreach { operator => mustHaveOperator(result.getExecutionPlanDescription, operator) }
      tx.commit()
      count
    } finally {
      tx.close()
    }
  }

  def mustHaveOperator(plan: ExecutionPlanDescription, operator: String): Unit = {
    withClue(s"The plan did not contain any $operator : ") {
      plan.asInstanceOf[PlanDescriptionImpl].find(operator).nonEmpty should be(true)
    }
  }

  val PASSWORD_CHANGE_REQUIRED_MESSAGE: String = "%n%nThe credentials you provided were valid, but must be " +
    "changed before you can " +
    "use this instance. If this is the first time you are using Neo4j, this is to " +
    "ensure you are not using the default credentials in production. If you are not " +
    "using default credentials, you are getting this message because an administrator " +
    "requires a password change.%n" +
    "Changing your password is easy to do via the Neo4j Browser.%n" +
    "If you are connecting via a shell or programmatically via a driver, " +
    "just issue a `ALTER CURRENT USER SET PASSWORD FROM 'current password' TO 'new password'` " +
    "statement against the system database in the current " +
    "session, and then restart your driver with the new password configured."

  // Setup methods/variables for starting with different settings
  val defaultConfig: Config = Config.defaults( GraphDatabaseSettings.auth_enabled, TRUE )

  var databaseDirectory: File = _

  def setup(config: Config = defaultConfig, impermanent: Boolean = true): Unit = {
    databaseDirectory = Files.createTempDirectory("test").toFile
    val builder = graphDatabaseFactory(databaseDirectory).setConfig(config).setInternalLogProvider(logProvider)
    if (impermanent) builder.impermanent()
    managementService = builder.build()
    graphOps = managementService.database(SYSTEM_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)

    initSystemGraph(config)
  }

  def restart(config: Config = defaultConfig): Unit = {
    managementService.shutdown()
    val builder = graphDatabaseFactory(databaseDirectory).setConfig(config).setInternalLogProvider(logProvider)
    managementService = builder.build()
    graphOps = managementService.database(SYSTEM_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)

    initSystemGraph(config)
  }

  def initSystemGraph(config: Config): Unit = {
    val databaseManager = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])
    val systemGraphInitializer = new EnterpriseSystemGraphInitializer(databaseManager, config)
    val securityGraphInitializer = new EnterpriseSecurityGraphInitializer(databaseManager,
      systemGraphInitializer,
      mock[Log],
      new InMemoryUserRepository,
      new InMemoryRoleRepository,
      new InMemoryUserRepository,
      new InMemoryUserRepository,
      new SecureHasher)
    securityGraphInitializer.initializeSecurityGraph()
    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  def clearPublicRole(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ACCESS ON DEFAULT DATABASE FROM $PUBLIC")
  }
}
