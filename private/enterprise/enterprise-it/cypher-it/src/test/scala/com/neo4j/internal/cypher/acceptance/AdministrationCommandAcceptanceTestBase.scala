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
  val DEFAULT: String = "DEFAULT"

  val neo4jUser: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN))
  val neo4jUserActive: Map[String, Any] = user("neo4j", Seq(PredefinedRoles.ADMIN), passwordChangeRequired = false)
  val onlineStatus: String = DatabaseStatus.Online.stringValue()
  val offlineStatus: String = DatabaseStatus.Offline.stringValue()

  val public: Map[String, Any] = role(PredefinedRoles.PUBLIC).map
  val admin: Map[String, Any] = role(PredefinedRoles.ADMIN).map
  val reader: Map[String, Any] = role(PredefinedRoles.READER).map

  val defaultRoles: Set[Map[String, Any]] = Set(
    public,
    admin,
    role(PredefinedRoles.ARCHITECT).map,
    role(PredefinedRoles.PUBLISHER).map,
    role(PredefinedRoles.EDITOR).map,
    reader
  )

  val adminWithDefaultUser: Map[String, Any] = role(PredefinedRoles.ADMIN).member("neo4j").map

  val defaultRolesWithUsers: Set[Map[String, Any]] = Set(
    role(PredefinedRoles.PUBLIC).member("neo4j").map,
    adminWithDefaultUser,
    role(PredefinedRoles.ARCHITECT).noMember().map,
    role(PredefinedRoles.PUBLISHER).noMember().map,
    role(PredefinedRoles.EDITOR).noMember().map,
    role(PredefinedRoles.READER).noMember().map
  )

  lazy val defaultRolePrivileges: Set[Map[String, AnyRef]] = Set(
    granted(access).database(DEFAULT).role(PredefinedRoles.PUBLIC).map,

    granted(access).role("reader").map,
    granted(matchPrivilege).role("reader").node("*").map,
    granted(matchPrivilege).role("reader").relationship("*").map,

    granted(access).role("editor").map,
    granted(matchPrivilege).role("editor").node("*").map,
    granted(matchPrivilege).role("editor").relationship("*").map,
    granted(write).role("editor").node("*").map,
    granted(write).role("editor").relationship("*").map,

    granted(access).role("publisher").map,
    granted(matchPrivilege).role("publisher").node("*").map,
    granted(matchPrivilege).role("publisher").relationship("*").map,
    granted(write).role("publisher").node("*").map,
    granted(write).role("publisher").relationship("*").map,
    granted(nameManagement).role("publisher").map,

    granted(access).role("architect").map,
    granted(matchPrivilege).role("architect").node("*").map,
    granted(matchPrivilege).role("architect").relationship("*").map,
    granted(write).role("architect").node("*").map,
    granted(write).role("architect").relationship("*").map,
    granted(nameManagement).role("architect").map,
    granted(indexManagement).role("architect").map,
    granted(constraintManagement).role("architect").map,

    granted(access).role("admin").map,
    granted(matchPrivilege).role("admin").node("*").map,
    granted(matchPrivilege).role("admin").relationship("*").map,
    granted(write).role("admin").node("*").map,
    granted(write).role("admin").relationship("*").map,
    granted(nameManagement).role("admin").map,
    granted(indexManagement).role("admin").map,
    granted(constraintManagement).role("admin").map,
    granted(adminPrivilege).role("admin").map,
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

  lazy val defaultUserPrivileges: Set[Map[String, AnyRef]] = Set(
    granted(access).database(DEFAULT).role(PUBLIC).user("neo4j").map,
    granted(access).role("admin").user("neo4j").map,
    granted(matchPrivilege).role("admin").user("neo4j").node("*").map,
    granted(matchPrivilege).role("admin").user("neo4j").relationship("*").map,
    granted(write).role("admin").user("neo4j").node("*").map,
    granted(write).role("admin").user("neo4j").relationship("*").map,
    granted(nameManagement).role("admin").user("neo4j").map,
    granted(indexManagement).role("admin").user("neo4j").map,
    granted(constraintManagement).role("admin").user("neo4j").map,
    granted(adminPrivilege).role("admin").user("neo4j").map,
  )

  def asPrivilegesResult(row: Result.ResultRow): Map[String, AnyRef] =
    Map(
      "access" -> row.get("access"),
      "action" -> row.get("action"),
      "resource" -> row.get("resource"),
      "graph" -> row.get("graph"),
      "segment" -> row.get("segment"),
      "role" -> row.get("role"),
      "user" -> row.get("user")
    )

  def authManager: EnterpriseAuthManager = graph.getDependencyResolver.resolveDependency(classOf[EnterpriseAuthManager])

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.auth_enabled -> TRUE)

  def user(username: String, roles: Seq[String] = Seq.empty, suspended: Boolean = false, passwordChangeRequired: Boolean = true): Map[String, Any] = {
    val rolesWithPublic = roles.sorted :+ PredefinedRoles.PUBLIC
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

  def setupUserWithCustomAdminRole(username: String = "joe", password: String = "soap", rolename: String = "custom"): Unit = {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    execute(s"CREATE ROLE $rolename AS COPY OF admin")
    execute(s"GRANT ROLE $rolename TO $username")
  }


  case class RoleMapBuilder(map: Map[String, Any]) {
    def member(user: String) = RoleMapBuilder(map + ("member" -> user))

    def noMember() = RoleMapBuilder(map + ("member" -> null))
  }

  def role(roleName: String): RoleMapBuilder = RoleMapBuilder(Map("role" -> roleName))

  def publicRole(users: String*): Set[Map[String, Any]] =
    users.map(u => role(PredefinedRoles.PUBLIC).member(u).map).toSet

  case class PrivilegeMapBuilder(map: Map[String, AnyRef]) {
    def action(action: String) = PrivilegeMapBuilder(map + ("action" -> action))

    def role(role: String) = PrivilegeMapBuilder(map + ("role" -> role))

    def node(label: String) = PrivilegeMapBuilder(map + ("segment" -> s"NODE($label)"))

    def relationship(relType: String) = PrivilegeMapBuilder(map + ("segment" -> s"RELATIONSHIP($relType)"))

    def database(database: String) = PrivilegeMapBuilder(map + ("graph" -> database))

    def user(user: String) = PrivilegeMapBuilder(map + ("user" -> user))

    def property(property: String) = PrivilegeMapBuilder(map + ("resource" -> s"property($property)"))
  }

  private val baseMap: Map[String, String] = Map("graph" -> "*", "segment" -> "database")

  def granted(privilegeMap: Map[String,String]): PrivilegeMapBuilder = PrivilegeMapBuilder( privilegeMap + ("access" -> "GRANTED"))
  def denied(privilegeMap: Map[String,String]): PrivilegeMapBuilder = PrivilegeMapBuilder( privilegeMap + ("access" -> "DENIED"))

  type privilegeFunction = Map[String, String] => PrivilegeMapBuilder

  def adminAction(action: String): Map[String, String] = baseMap + ("resource" -> "database", "action" -> action)

  val startDatabase: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "start_database")
  val stopDatabase: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "stop_database")

  val createIndex: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "create_index")
  val dropIndex: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "drop_index")
  val indexManagement: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "index")

  val createConstraint: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "create_constraint")
  val dropConstraint: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "drop_constraint")
  val constraintManagement: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "constraint")

  val createNodeLabel: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "create_label")
  val createRelationshipType: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "create_reltype")
  val createPropertyKey: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "create_propertykey")
  val nameManagement: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "token")

  val access: Map[String, String] = baseMap + ("resource" -> "database", "action" ->"access")
  val traverse: Map[String, String] = baseMap + ("resource" -> "graph", "action" -> "traverse")
  val read: Map[String, String] = baseMap + ("resource" -> "all_properties", "action" -> "read")
  val matchPrivilege: Map[String, String] = baseMap + ("resource" -> "all_properties", "action" -> "match")
  val write: Map[String, String] = baseMap + ("resource" -> "graph", "action" -> "write")

  val allDatabasePrivilege: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "database_actions")
  val adminPrivilege:  Map[String, String] = baseMap + ("resource" -> "database", "action" -> "admin")

  def showTransaction(username: String): Map[String, String] =
    baseMap + ("resource" -> "database", "segment" -> s"USER($username)", "action" -> "show_transaction")
  def terminateTransaction(username: String): Map[String, String] =
    baseMap + ("resource" -> "database", "segment" -> s"USER($username)", "action" -> "terminate_transaction")
  def transaction(username: String): Map[String, String] =
    baseMap + ("resource" -> "database", "segment" -> s"USER($username)", "action" -> "transaction_management")

  // Collection of all dbms privileges

  val dbmsCommands: Seq[String] = Seq(
    "CREATE ROLE",
    "DROP ROLE",
    "ASSIGN ROLE",
    "REMOVE ROLE",
    "SHOW ROLE",
    "ROLE MANAGEMENT",
    "CREATE USER",
    "DROP USER",
    "SHOW USER",
    "SET USER STATUS",
    "SET PASSWORDS",
    "ALTER USER",
    "USER MANAGEMENT",
    "CREATE DATABASE",
    "DROP DATABASE",
    "DATABASE MANAGEMENT",
    "SHOW PRIVILEGE",
    "ASSIGN PRIVILEGE",
    "REMOVE PRIVILEGE",
    "PRIVILEGE MANAGEMENT",
    "ALL DBMS PRIVILEGES"
  )

  // Must be in the same order as dbmsCommands
  val dbmsActions: Seq[Map[String, String]] = Seq(
    adminAction("create_role"),
    adminAction("drop_role"),
    adminAction("assign_role"),
    adminAction("remove_role"),
    adminAction("show_role"),
    adminAction("role_management"),
    adminAction("create_user"),
    adminAction("drop_user"),
    adminAction("show_user"),
    adminAction("set_user_status"),
    adminAction("set_passwords"),
    adminAction("alter_user"),
    adminAction("user_management"),
    adminAction("create_database"),
    adminAction("drop_database"),
    adminAction("database_management"),
    adminAction("show_privilege"),
    adminAction("assign_privilege"),
    adminAction("remove_privilege"),
    adminAction("privilege_management"),
    adminAction("dbms_actions")
  )

  // Collection of all database privileges

  val basicDatabaseCommands: Seq[String] = Seq(
    "ACCESS",
    "START",
    "STOP"
  )

  // Must be in the same order as basicDatabaseCommands
  private val basicDatabaseActions: Seq[Map[String, String]] = Seq(
    access,
    startDatabase,
    stopDatabase
  )

  val schemaCommands = Seq(
    "CREATE INDEX",
    "DROP INDEX",
    "INDEX MANAGEMENT",
    "CREATE CONSTRAINT",
    "DROP CONSTRAINT",
    "CONSTRAINT MANAGEMENT",
    "CREATE NEW NODE LABEL",
    "CREATE NEW RELATIONSHIP TYPE",
    "CREATE NEW PROPERTY NAME",
    "NAME MANAGEMENT",
    "ALL DATABASE PRIVILEGES"
  )

  // Must be in the same order as schemaCommands
  val schemaActions: Seq[Map[String, String]] = Seq(
    createIndex,
    dropIndex,
    indexManagement,
    createConstraint,
    dropConstraint,
    constraintManagement,
    createNodeLabel,
    createRelationshipType,
    createPropertyKey,
    nameManagement,
    allDatabasePrivilege
  )

  // Must be in the same order as transactionCommands
  val transactionCommands = Seq(
    "SHOW TRANSACTION",
    "TERMINATE TRANSACTION",
    "TRANSACTION MANAGEMENT"
  )

  private val transactionActions: Seq[Map[String, String]] = Seq(
    showTransaction("*"),
    terminateTransaction("*"),
    transaction("*")
  )

  // Collection of all kinds of graph privileges

  val graphCommands = Seq(
    "TRAVERSE ON GRAPH * NODES A",
    "READ {prop} ON GRAPH * NODES *",
    "MATCH {prop} ON GRAPH * NODES A ",
    "WRITE ON GRAPH *"
  )

  private val graphActions = Seq(
    Set(traverse ++ Map("segment" -> "NODE(A)")),
    Set(read ++ Map("segment" -> "NODE(*)", "resource" -> "property(prop)")),
    Set(matchPrivilege ++ Map("segment" -> "NODE(A)", "resource" -> "property(prop)")),
    Set(write ++ Map("segment" -> "NODE(*)"), write ++ Map("segment" -> "RELATIONSHIP(*)"))
  )

  // Collection of database, dbms and graph privileges

  val allPrivilegeCommands: Seq[String] = (basicDatabaseCommands ++ schemaCommands ++ transactionCommands).map(command => command + " ON DATABASE *") ++
    dbmsCommands.map(command => command + " ON DBMS")

  private val allPrivilegeActions = (basicDatabaseActions ++ schemaActions ++ transactionActions ++ dbmsActions).map(action => Set(action)) ++ graphActions
  val allPrivileges: Seq[(String, Set[Map[String, String]])] = allPrivilegeCommands zip allPrivilegeActions

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
      new SecureHasher,
      config)
    securityGraphInitializer.initializeSecurityGraph()
    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  def clearPublicRole(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ACCESS ON DEFAULT DATABASE FROM ${PredefinedRoles.PUBLIC}")
  }

  def createRoleWithOnlyAdminPrivilege(name: String = "adminOnly"): Unit = {
    execute(s"CREATE ROLE $name AS COPY OF admin")
    execute(s"REVOKE MATCH {*} ON GRAPH * FROM $name")
    execute(s"REVOKE WRITE ON GRAPH * FROM $name")
    execute(s"REVOKE ACCESS ON DATABASE * FROM $name")
    execute(s"REVOKE ALL ON DATABASE * FROM $name")
    execute(s"REVOKE NAME ON DATABASE * FROM $name")
    execute(s"REVOKE INDEX ON DATABASE * FROM $name")
    execute(s"REVOKE CONSTRAINT ON DATABASE * FROM $name")
    execute(s"SHOW ROLE $name PRIVILEGES").toSet should be(Set(granted(adminPrivilege).role(name).map))
  }

  override protected def initTest() {
    super.initTest()
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
  }
}
