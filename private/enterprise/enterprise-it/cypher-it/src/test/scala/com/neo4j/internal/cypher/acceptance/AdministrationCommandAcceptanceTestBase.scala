/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE
import java.nio.file.Files
import java.nio.file.Path
import java.util
import java.util.Collections

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import com.neo4j.dbms.EnterpriseSystemGraphComponent
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository
import com.neo4j.server.security.enterprise.auth.PrivilegeResolver
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.dbms.database.DefaultSystemGraphInitializer
import org.neo4j.dbms.database.SystemGraphComponents
import org.neo4j.fabric.executor.TaggingPlanDescriptionWrapper
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.InMemoryUserRepository
import org.neo4j.server.security.auth.SecurityTestUtils
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent

abstract class AdministrationCommandAcceptanceTestBase extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {
  val DEFAULT: String = "DEFAULT"
  val FAIL_EXECUTE_ADMIN_PROC: String = "Executing admin procedure is not allowed"
  val FAIL_EXECUTE_PROC: String = "Executing procedure is not allowed"
  val FAIL_EXECUTE_FUNC: String = "Executing a user defined function is not allowed"
  val FAIL_EXECUTE_AGG_FUNC: String = "Executing a user defined aggregating function is not allowed"

  private val helpfulCheckUserPrivilegeErrorText: String = " Try executing SHOW USER PRIVILEGES to determine the missing or denied privileges. " +
    "In case of missing privileges, they need to be granted (See GRANT). In case of denied privileges, they need to be revoked (See REVOKE) and granted."

  val PERMISSION_DENIED_SHOW_USER: String = "Permission denied for SHOW USER." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SHOW_PRIVILEGE: String = "Permission denied for SHOW PRIVILEGE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SHOW_PRIVILEGE_OR_USER: String = "Permission denied for SHOW PRIVILEGE and/or SHOW USER." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_ASSIGN_PRIVILEGE: String = "Permission denied for ASSIGN PRIVILEGE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_REMOVE_PRIVILEGE: String = "Permission denied for REMOVE PRIVILEGE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_START: String = "Permission denied for START DATABASE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_STOP: String = "Permission denied for STOP DATABASE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_CREATE_DATABASE: String = "Permission denied for CREATE DATABASE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_DROP_DATABASE: String = "Permission denied for DROP DATABASE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_CREATE_OR_DROP_DATABASE: String = "Permission denied for CREATE DATABASE and/or DROP DATABASE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_CREATE_ROLE: String = "Permission denied for CREATE ROLE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_DROP_ROLE: String = "Permission denied for DROP ROLE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_CREATE_OR_DROP_ROLE: String = "Permission denied for CREATE ROLE and/or DROP ROLE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_CREATE_USER: String = "Permission denied for CREATE USER." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_DROP_USER: String = "Permission denied for DROP USER." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_CREATE_OR_DROP_USER: String = "Permission denied for CREATE USER and/or DROP USER." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SHOW_ROLE: String = "Permission denied for SHOW ROLE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SHOW_ROLE_OR_USERS: String = "Permission denied for SHOW ROLE and/or SHOW USER." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_ASSIGN_ROLE: String = "Permission denied for ASSIGN ROLE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_REMOVE_ROLE: String = "Permission denied for REMOVE ROLE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SET_PASSWORDS: String = "Permission denied for SET PASSWORDS." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SET_USER_STATUS: String = "Permission denied for SET USER STATUS." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SET_USER_DEFAULT_DATABASE: String = "Permission denied for SET USER DEFAULT DATABASE." + helpfulCheckUserPrivilegeErrorText
  val PERMISSION_DENIED_SET_PASSWORDS_OR_USER_STATUS: String = "Permission denied for SET PASSWORDS and/or SET USER STATUS." + helpfulCheckUserPrivilegeErrorText

  val roleName: String = "custom"
  val roleName2: String = "otherRole"
  val username: String = "joe"
  val password: String = "soap"
  val newPassword: String = "new"
  val databaseString: String = "foo"
  val databaseString2: String = "bar"

  val defaultUsername: String = "neo4j"
  val defaultUser: Map[String, Any] = adminUser(defaultUsername)
  val defaultUserActive: Map[String, Any] = adminUser(defaultUsername, passwordChangeRequired = false)
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

  val adminWithDefaultUser: Map[String, Any] = role(PredefinedRoles.ADMIN).member(defaultUsername).map

  val defaultRolesWithUsers: Set[Map[String, Any]] = Set(
    role(PredefinedRoles.PUBLIC).member(defaultUsername).map,
    adminWithDefaultUser,
    role(PredefinedRoles.ARCHITECT).noMember().map,
    role(PredefinedRoles.PUBLISHER).noMember().map,
    role(PredefinedRoles.EDITOR).noMember().map,
    role(PredefinedRoles.READER).noMember().map
  )

  lazy val defaultRolePrivileges: Set[Map[String, AnyRef]] = Set(
    granted(access).database(DEFAULT).role(PredefinedRoles.PUBLIC).map,
    granted(executeProcedure).role(PredefinedRoles.PUBLIC).map,
    granted(executeFunction).role(PredefinedRoles.PUBLIC).map,

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
    granted(startDatabase).role("admin").map,
    granted(stopDatabase).role("admin").map,
    granted(transaction("*")).role("admin").map,
    granted(adminAction("dbms_actions")).role("admin").map,
  )

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

  lazy val defaultUserPrivileges: Set[Map[String, String]] = publicPrivileges(defaultUsername) ++ Set(
    granted(access).role("admin").user(defaultUsername).map,
    granted(matchPrivilege).role("admin").user(defaultUsername).node("*").map,
    granted(matchPrivilege).role("admin").user(defaultUsername).relationship("*").map,
    granted(write).role("admin").user(defaultUsername).node("*").map,
    granted(write).role("admin").user(defaultUsername).relationship("*").map,
    granted(nameManagement).role("admin").user(defaultUsername).map,
    granted(indexManagement).role("admin").user(defaultUsername).map,
    granted(constraintManagement).role("admin").user(defaultUsername).map,
    granted(startDatabase).role("admin").user(defaultUsername).map,
    granted(stopDatabase).role("admin").user(defaultUsername).map,
    granted(transaction("*")).role("admin").user(defaultUsername).map,
    granted(adminAction("dbms_actions")).role("admin").user(defaultUsername).map,
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

  def user(username: String, roles: Seq[String] = Seq.empty, suspended: Boolean = false,
           passwordChangeRequired: Boolean = true, defaultDatabase: String = DEFAULT_DATABASE_NAME): Map[String, Any] = {
    val rolesWithPublic = roles.sorted :+ PredefinedRoles.PUBLIC
    Map("user" -> username, "roles" -> rolesWithPublic, "suspended" -> suspended, "passwordChangeRequired" -> passwordChangeRequired,
      "defaultDatabase" -> defaultDatabase)
  }

  def adminUser(username: String, passwordChangeRequired: Boolean = true, defaultDatabase: String = DEFAULT_DATABASE_NAME): Map[String, Any] = {
    val rolesWithPublic = Seq(PredefinedRoles.ADMIN, PredefinedRoles.PUBLIC)
    Map("user" -> username, "roles" -> rolesWithPublic, "suspended" -> false, "passwordChangeRequired" -> passwordChangeRequired,
      "defaultDatabase" -> defaultDatabase)
  }

  def db(name: String, status: String = onlineStatus, default: Boolean = false, systemDefault: Boolean = false): Map[String, Any] =
    Map("name" -> name,
      "address" -> "localhost:7687",
      "role" -> "standalone",
      "requestedStatus" -> status,
      "currentStatus" -> status,
      "error" -> "",
      "default" -> default,
      "systemDefault" -> systemDefault)

  def defaultDb(name: String = DEFAULT_DATABASE_NAME, status: String = onlineStatus): Map[String, String] =
    Map("name" -> name,
      "address" -> "localhost:7687",
      "role" -> "standalone",
      "requestedStatus" -> status,
      "currentStatus" -> status,
      "error" -> "")

  def setupUserWithCustomRole(username: String = username, password: String = password, rolename: String = roleName, access: Boolean = true): Unit = {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    execute(s"CREATE ROLE $rolename IF NOT EXISTS")
    execute(s"GRANT ROLE $rolename TO $username")
    if (access) execute(s"GRANT ACCESS ON DATABASE * TO $rolename")
  }

  def setupUserWithCustomAdminRole(username: String = username, password: String = password, rolename: String = roleName): Unit = {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    execute(s"CREATE ROLE $rolename AS COPY OF admin")
    execute(s"GRANT ROLE $rolename TO $username")
  }

  case class RoleMapBuilder(map: Map[String, Any]) {
    def member(user: String): RoleMapBuilder = RoleMapBuilder(map + ("member" -> user))

    def noMember(): RoleMapBuilder = RoleMapBuilder(map + ("member" -> null))
  }

  def role(roleName: String): RoleMapBuilder = RoleMapBuilder(Map("role" -> roleName))

  def role(predefinedRole: PredefinedRoles): RoleMapBuilder = RoleMapBuilder(Map("role" -> predefinedRole))

  def publicRole(users: String*): Set[Map[String, Any]] =
    users.map(u => role(PredefinedRoles.PUBLIC).member(u).map).toSet

  def publicPrivileges(user: String): Set[Map[String, String]] = Set(
    granted(executeProcedure).user(user).role(PUBLIC).map,
    granted(executeFunction).user(user).role(PUBLIC).map,
    granted(access).database(DEFAULT).user(user).role(PUBLIC).map
  )

  case class PrivilegeMapBuilder(map: Map[String, String]) {
    def action(action: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("action" -> action))

    def role(role: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("role" -> role))

    def node(label: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("segment" -> s"NODE($label)"))

    def relationship(relType: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("segment" -> s"RELATIONSHIP($relType)"))

    def procedure(procedure: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("segment" -> s"PROCEDURE($procedure)"))

    def function(function: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("segment" -> s"FUNCTION($function)"))

    /**
     * Currently database() and graph() are equivalent.
     * But when/if we implement multiple graphs per database, the distinction will become important.
     * So try to use the correct one already now.
     */

    def database(database: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("graph" -> database))

    def graph(graph: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("graph" -> graph))

    def user(user: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("user" -> user))

    def property(property: String): PrivilegeMapBuilder = PrivilegeMapBuilder(map + ("resource" -> s"property($property)"))

    def label(label: String): PrivilegeMapBuilder = {
      val labelResource = if (label.equals("*"))  "all_labels" else s"label($label)"
      PrivilegeMapBuilder(map + ("resource" -> labelResource))
    }
  }

  private val baseMap: Map[String, String] = Map("graph" -> "*", "segment" -> "database")

  def grantedFromConfig(name: String, role: String): Set[Map[String, String]] = Set(
    granted(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).function(name).role(role).map,
    granted(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).procedure(name).role(role).map
  )

  def grantedFromConfig(name: String, role: String, user: String): Set[Map[String, String]] = Set(
    granted(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).function(name).role(role).user(user).map,
    granted(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).procedure(name).role(role).user(user).map
  )

  def deniedFromConfig(name: String, role: String): Set[Map[String, String]] = Set(
    denied(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).function(name).role(role).map,
    denied(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).procedure(name).role(role).map
  )

  def deniedFromConfig(name: String, role: String, user: String): Set[Map[String, String]] = Set(
    denied(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).function(name).role(role).user(user).map,
    denied(adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)).procedure(name).role(role).user(user).map
  )

  def granted(privilegeMap: Map[String,String]): PrivilegeMapBuilder = PrivilegeMapBuilder( privilegeMap + ("access" -> "GRANTED"))
  def denied(privilegeMap: Map[String,String]): PrivilegeMapBuilder = PrivilegeMapBuilder( privilegeMap + ("access" -> "DENIED"))

  type privilegeFunction = Map[String, String] => PrivilegeMapBuilder

  def adminAction(action: String): Map[String, String] = baseMap + ("resource" -> "database", "action" -> action)

  val executeProcedure: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "execute", "segment" -> "PROCEDURE(*)")
  val executeBoosted: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "execute_boosted", "segment" -> "PROCEDURE(*)")
  val executeBoostedFromConfig: Map[String, String] = adminAction(PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG)

  val executeFunction: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "execute", "segment" -> "FUNCTION(*)")
  val executeBoostedFunction: Map[String, String] = baseMap + ("resource" -> "database", "action" -> "execute_boosted", "segment" -> "FUNCTION(*)")

  val startDatabase: Map[String, String] = adminAction("start_database")
  val stopDatabase: Map[String, String] = adminAction("stop_database")

  val createIndex: Map[String, String] = adminAction("create_index")
  val dropIndex: Map[String, String] = adminAction("drop_index")
  val showIndex: Map[String, String] = adminAction("show_index")
  val indexManagement: Map[String, String] = adminAction("index")

  val createConstraint: Map[String, String] = adminAction("create_constraint")
  val dropConstraint: Map[String, String] = adminAction("drop_constraint")
  val showConstraint: Map[String, String] = adminAction("show_constraint")
  val constraintManagement: Map[String, String] = adminAction("constraint")

  val createNodeLabel: Map[String, String] = adminAction("create_label")
  val createRelationshipType: Map[String, String] = adminAction("create_reltype")
  val createPropertyKey: Map[String, String] = adminAction("create_propertykey")
  val nameManagement: Map[String, String] = adminAction("token")

  val access: Map[String, String] = adminAction("access")
  val traverse: Map[String, String] = baseMap + ("resource" -> "graph", "action" -> "traverse")
  val read: Map[String, String] = baseMap + ("resource" -> "all_properties", "action" -> "read")
  val matchPrivilege: Map[String, String] = baseMap + ("resource" -> "all_properties", "action" -> "match")
  val write: Map[String, String] = baseMap + ("resource" -> "graph", "action" -> "write")
  val create: Map[String, String] = baseMap + ("resource" -> "graph", "action" -> "create_element")
  val delete: Map[String, String] = baseMap + ("resource" -> "graph", "action" -> "delete_element")
  val setProperty: Map[String, String] = baseMap + ("resource" -> "all_properties", "action" -> "set_property")
  val merge: Map[String, String] = baseMap + ("resource" -> "all_properties", "action" -> "merge")
  val setLabel: Map[String, String] = baseMap + ("action" -> "set_label", "segment" -> "NODE(*)")
  val removeLabel: Map[String, String] = baseMap + ("action" -> "remove_label", "segment" -> "NODE(*)")
  val allGraphPrivileges: Map[String, String] = baseMap + ("resource" -> "graph", "action" -> "graph_actions")

  def showTransaction(username: String): Map[String, String] =
    baseMap + ("resource" -> "database", "segment" -> s"USER($username)", "action" -> "show_transaction")
  def terminateTransaction(username: String): Map[String, String] =
    baseMap + ("resource" -> "database", "segment" -> s"USER($username)", "action" -> "terminate_transaction")
  def transaction(username: String): Map[String, String] =
    baseMap + ("resource" -> "database", "segment" -> s"USER($username)", "action" -> "transaction_management")

  // Collection of all dbms privileges

  val dbmsPrivileges: Map[String, Map[String, String]] = Map(
    "CREATE ROLE" -> adminAction("create_role"),
    "DROP ROLE" -> adminAction("drop_role"),
    "ASSIGN ROLE" -> adminAction("assign_role"),
    "REMOVE ROLE" -> adminAction("remove_role"),
    "SHOW ROLE" -> adminAction("show_role"),
    "ROLE MANAGEMENT" -> adminAction("role_management"),
    "CREATE USER" -> adminAction("create_user"),
    "DROP USER" -> adminAction("drop_user"),
    "SHOW USER" -> adminAction("show_user"),
    "SET USER STATUS" -> adminAction("set_user_status"),
    "SET PASSWORDS" -> adminAction("set_passwords"),
    "ALTER USER" -> adminAction("alter_user"),
    "USER MANAGEMENT" -> adminAction("user_management"),
    "CREATE DATABASE" -> adminAction("create_database"),
    "DROP DATABASE" -> adminAction("drop_database"),
    "DATABASE MANAGEMENT" -> adminAction("database_management"),
    "SHOW PRIVILEGE" -> adminAction("show_privilege"),
    "ASSIGN PRIVILEGE" -> adminAction("assign_privilege"),
    "REMOVE PRIVILEGE" -> adminAction("remove_privilege"),
    "PRIVILEGE MANAGEMENT" -> adminAction("privilege_management"),
    "ALL DBMS PRIVILEGES" -> adminAction("dbms_actions"),
    "EXECUTE ADMIN PROCEDURES" -> adminAction("execute_admin")
  )
  val dbmsCommands: Iterable[String] = dbmsPrivileges.keys

  val executeProcedurePrivileges: Map[String, Map[String, String]] = Map(
    "EXECUTE PROCEDURE" -> executeProcedure,
    "EXECUTE BOOSTED PROCEDURE" -> executeBoosted
  )

  val executeFunctionPrivileges: Map[String, Map[String, String]] = Map(
    "EXECUTE FUNCTION" -> executeFunction,
    "EXECUTE BOOSTED FUNCTION" -> executeBoostedFunction
  )

  val executePrivileges: Map[String, Map[String, String]] = executeProcedurePrivileges ++ executeFunctionPrivileges

  // Collection of all database privileges

  val basicDatabasePrivileges: Map[String, Map[String, String]] = Map(
    "ACCESS" -> access,
    "START" -> startDatabase,
    "STOP" -> stopDatabase
  )

  val schemaPrivileges: Map[String, Map[String, String]] = Map(
    "CREATE INDEX" -> createIndex,
    "DROP INDEX" -> dropIndex,
    "SHOW INDEX" -> showIndex,
    "INDEX MANAGEMENT" -> indexManagement,
    "CREATE CONSTRAINT" -> createConstraint,
    "DROP CONSTRAINT" -> dropConstraint,
    "SHOW CONSTRAINT" -> showConstraint,
    "CONSTRAINT MANAGEMENT" -> constraintManagement,
    "CREATE NEW NODE LABEL" -> createNodeLabel,
    "CREATE NEW RELATIONSHIP TYPE" -> createRelationshipType,
    "CREATE NEW PROPERTY NAME" -> createPropertyKey,
    "NAME MANAGEMENT" -> nameManagement,
    "ALL DATABASE PRIVILEGES" -> adminAction("database_actions")
  )

  val transactionPrivileges: Map[String, Map[String, String]] = Map(
    "SHOW TRANSACTION" -> showTransaction("*"),
    "TERMINATE TRANSACTION" -> terminateTransaction("*"),
    "TRANSACTION MANAGEMENT" -> transaction("*")
  )

  // Collection of all kinds of graph privileges,
  // except MERGE since that is not valid for (REVOKE) DENY
  val graphPrivileges: Map[String, Set[Map[String, String]]] = Map(
    "TRAVERSE ON GRAPH * NODES A" ->  Set(traverse ++ Map("segment" -> "NODE(A)")),
    "READ {prop} ON GRAPH * NODES *" -> Set(read ++ Map("segment" -> "NODE(*)", "resource" -> "property(prop)")),
    "MATCH {prop} ON GRAPH * NODES A " -> Set(matchPrivilege ++ Map("segment" -> "NODE(A)", "resource" -> "property(prop)")),
    "WRITE ON GRAPH *" -> Set(write ++ Map("segment" -> "NODE(*)"), write ++ Map("segment" -> "RELATIONSHIP(*)")),
    "SET LABEL foo ON DEFAULT GRAPH" -> Set(setLabel ++ Map("resource" -> "label(foo)", "graph" -> "DEFAULT")),
    "REMOVE LABEL * ON GRAPH neo4j" -> Set(removeLabel ++ Map("resource" -> "all_labels", "graph" -> "neo4j")),
    "CREATE ON GRAPH * RELATIONSHIPS *" -> Set(create ++ Map("segment" -> "RELATIONSHIP(*)")),
    "DELETE ON GRAPHS * NODE A" -> Set(delete ++ Map("segment" -> "NODE(A)")),
    "SET PROPERTY {prop} ON DEFAULT GRAPH" -> Set(setProperty ++ Map("segment" -> "NODE(*)", "resource" -> "property(prop)", "graph" -> "DEFAULT"),
                                                  setProperty ++ Map("segment" -> "RELATIONSHIP(*)", "resource" -> "property(prop)", "graph" -> "DEFAULT")),
    "ALL GRAPH PRIVILEGES ON GRAPH *" -> Set(allGraphPrivileges)
  )

  // Collection of all kind of privileges

  val allPrivileges: Map[String, Set[Map[String, String]]] =
    (basicDatabasePrivileges ++ schemaPrivileges ++ transactionPrivileges).map {
      case (command, action) => (command + " ON DATABASE *", Set(action))
    } ++
    dbmsPrivileges.map {
      case (command, action) => (command + " ON DBMS", Set(action))
    } ++
    executePrivileges.map {
      case (command, action) => (command + " * ON DBMS", Set(action))
    } ++
    graphPrivileges.map {
      case (command, action) => (command, action)
    }

  val allPrivilegeCommands: Iterable[String] = allPrivileges.keys

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

  def executeOnDBMSDefault(username: String, password: String, query: String,
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
      val unwrapped = plan match {
        case wrapper: TaggingPlanDescriptionWrapper => wrapper.getInnerPlanDescription
        case desc                                   => desc
      }
      unwrapped.asInstanceOf[PlanDescriptionImpl].find(operator).nonEmpty should be(true)
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

  private var databaseDirectory: Path = _

  def setup(config: Config = defaultConfig, impermanent: Boolean = true): Unit = {
    databaseDirectory = Files.createTempDirectory("test")
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

  def initSystemGraph(config: Config = defaultConfig): Unit = {
    val databaseManager = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])
    val systemGraphSupplier = SystemGraphRealmHelper.makeSystemSupplier(databaseManager)

    val userRepository = new InMemoryUserRepository
    val userSecurityGraphComponent = new UserSecurityGraphComponent(mock[Log], userRepository, userRepository, config)

    val systemGraphComponents = new SystemGraphComponents()
    systemGraphComponents.register(new EnterpriseSystemGraphComponent(config))
    systemGraphComponents.register(userSecurityGraphComponent)
    val systemGraphInitializer = new DefaultSystemGraphInitializer(systemGraphSupplier, systemGraphComponents)
    systemGraphInitializer.start()

    selectDatabase(SYSTEM_DATABASE_NAME)

    // we started the enterprise security graph component separately to allow tests to override this behaviour for version testing
    initializeEnterpriseSecurityGraphComponent(config)
  }

  def initializeEnterpriseSecurityGraphComponent(config: Config): Unit = {
    val userRepository = new InMemoryUserRepository
    val roleRepository = new InMemoryRoleRepository
    val enterpriseSecurityGraphComponent = new EnterpriseSecurityGraphComponent(mock[Log], roleRepository, userRepository, config)
    enterpriseSecurityGraphComponent.initializeSystemGraph(graphOps, true)
  }

  def clearPublicRole(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ACCESS ON DEFAULT DATABASE FROM ${PredefinedRoles.PUBLIC}")
    execute(s"REVOKE EXECUTE PROCEDURES * ON DBMS FROM ${PredefinedRoles.PUBLIC}")
    execute(s"REVOKE EXECUTE FUNCTION * ON DBMS FROM ${PredefinedRoles.PUBLIC}")
    execute("SHOW ROLE PUBLIC PRIVILEGES").toList should be(empty)
  }

  override protected def initTest() {
    super.initTest()
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
  }
}
