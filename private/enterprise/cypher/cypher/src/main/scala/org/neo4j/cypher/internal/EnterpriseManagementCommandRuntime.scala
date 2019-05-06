/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import com.neo4j.server.security.enterprise.auth.{CommercialAuthAndUserManager, EnterpriseUserManager}
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs.{NoResultSystemCommandExecutionPlan, SystemCommandExecutionPlan, UpdatingSystemCommandExecutionPlan}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.string.UTF8
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

/**
  * This runtime takes on queries that require no planning, such as multidatabase management commands
  */
case class EnterpriseManagementCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver) extends ManagementCommandRuntime {
  val communityCommandRuntime: CommunityManagementCommandRuntime = CommunityManagementCommandRuntime(normalExecutionEngine)

  override def name: String = "enterprise management-commands"

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext): ExecutionPlan = {

    def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
      throw new CantCompileQueryException(
        s"Plan is not a recognized database administration command: ${unknownPlan.getClass.getSimpleName}")
    }

    val (withSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    //TODO: Test this with overlapping commands
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable)
      .applyOrElse(withSlottedParameters, throwCantCompile).apply(context, parameterMapping)
  }

  private lazy val userManager: EnterpriseUserManager = {
    val supplier = resolver.resolveDependency(classOf[CommercialAuthAndUserManager])
    //      val supplier = normalExecutionEngine.queryService.getDependencyResolver.resolveDependency(classOf[CommercialAuthAndUserManager])
    supplier.getUserManager
  }

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, Map[String, Int]) => ExecutionPlan] = {
    // SHOW USERS
    case ShowUsers() => (_, _) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |RETURN u.name as user, collect(r.name) as roles""".stripMargin,
        VirtualValues.EMPTY_MAP
      )

    // CREATE USER foo WITH PASSWORD password
    case CreateUser(userName, Some(initialStringPassword), None, requirePasswordChange, suspended) => (_, _) =>
      userManager.newUser(userName, UTF8.encode(initialStringPassword), requirePasswordChange)
      // Default value is not suspended, so only set suspended if needed
      if (suspended) userManager.setUserStatus(userName, suspended)
      NoResultSystemCommandExecutionPlan()

    // CREATE USER foo WITH PASSWORD $password
    case CreateUser(_, _, Some(_), _, _) =>
      throw new IllegalStateException("Did not resolve parameters correctly.")

    // CREATE USER foo WITH PASSWORD
    case CreateUser(_, _, _, _, _) =>
      throw new IllegalStateException("Password not correctly supplied.")

    // DROP USER foo
    case DropUser(userName) => (_, _) =>
      userManager.deleteUser(userName)
      NoResultSystemCommandExecutionPlan()

    // ALTER USER foo
    case AlterUser(userName, initialStringPassword, None, requirePasswordChange, suspended) => (_, _) =>
      if (suspended.isDefined)
        userManager.setUserStatus(userName, suspended.get)

      val newPassword: String = initialStringPassword.orNull
      if (requirePasswordChange.isDefined && newPassword != null)
        // change both password and requirePasswordChange
        userManager.setUserPassword(userName, UTF8.encode(newPassword), requirePasswordChange.get)
      else if (requirePasswordChange.isDefined)
        // change only mode
        userManager.setUserRequirePasswordChange(userName, requirePasswordChange.get)
      else if (newPassword != null) {
        // change only password
        val changePassword = userManager.getUser(userName).passwordChangeRequired()
        userManager.setUserPassword(userName, UTF8.encode(newPassword), changePassword)
      }
      NoResultSystemCommandExecutionPlan()

    // ALTER USER foo
    case AlterUser(_, _, Some(_), _, _) =>
      throw new IllegalStateException("Did not resolve parameters correctly.")

    // SHOW [ ALL | POPULATED ] ROLES [ WITH USERS ]
    case ShowRoles(withUsers, showAll) => (_, _) =>
      // TODO fix the predefined roles to be connected to PredefinedRoles (aka PredefinedRoles.ADMIN)
      val predefinedRoles = Values.stringArray("admin", "architect", "publisher", "editor", "reader")
      val query = if (showAll)
                        """MATCH (r:Role)
                          |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r)
                          |RETURN DISTINCT r.name as role,
                          |CASE
                          | WHEN r.name IN $predefined THEN true
                          | ELSE false
                          |END as is_built_in
                        """.stripMargin
                      else
                        """MATCH (r:Role)<-[:HAS_ROLE]-(u:User)
                          |RETURN DISTINCT r.name as role,
                          |CASE
                          | WHEN r.name IN $predefined THEN true
                          | ELSE false
                          |END as is_built_in
                        """.stripMargin
      if (withUsers) {
        SystemCommandExecutionPlan("ShowRoles", normalExecutionEngine, query + ", u.name as member",
          VirtualValues.map(Array("predefined"), Array(predefinedRoles))
        )
      } else {
        SystemCommandExecutionPlan("ShowRoles", normalExecutionEngine, query,
          VirtualValues.map(Array("predefined"), Array(predefinedRoles))
        )
      }

    // CREATE ROLE foo AS COPY OF bar
    case CreateRole(roleName, Some(from)) => (_, _) =>
      userManager.newCopyOfRole(roleName, from)
      NoResultSystemCommandExecutionPlan()

    // CREATE ROLE foo
    case CreateRole(roleName, _) => (_, _) =>
      userManager.newRole(roleName)
      NoResultSystemCommandExecutionPlan()

    // DROP ROLE foo
    case DropRole(roleName) => (_, _) =>
      userManager.deleteRole(roleName)
      NoResultSystemCommandExecutionPlan()

    // SHOW DATABASES
    case ShowDatabases() => (_, _) =>
      SystemCommandExecutionPlan("ShowDatabases", normalExecutionEngine,
        "MATCH (d:Database) RETURN d.name as name, d.status as status", VirtualValues.emptyMap())

    // SHOW DATABASE foo
    case ShowDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("ShowDatabase", normalExecutionEngine,
        "MATCH (d:Database {name: $name}) RETURN d.name as name, d.status as status", VirtualValues.map(Array("name"), Array(Values.stringValue(dbName))))

    // CREATE DATABASE foo
    case CreateDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        """CREATE (d:Database {name: $name})
          |SET d.status = $status
          |SET d.created_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name", "status"), Array(Values.stringValue(dbName), DatabaseStatus.Online))
      )

    // DROP DATABASE foo
    case DropDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d:Database {name: $name})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(dbName))),
        record => {
          if (record.get("name") == null) throw new InvalidArgumentsException("Database '" + dbName + "' does not exist.")
        }
      )

      // TODO: Check back if Start/Stop should also be "just" in enterprise or not. Check also CommunityManagementCommandRuntime
    /*// START DATABASE foo
    case StartDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("StartDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d:Database {name: $name})
          |OPTIONAL MATCH (d2:Database {name: $name, status: $oldStatus})
          |SET d2.status = $status
          |SET d2.started_at = datetime()
          |RETURN d2.name as name, d2.status as status, d.name as db""".stripMargin,
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.stringValue(dbName),
            DatabaseStatus.Offline,
            DatabaseStatus.Online
          )
        ),
        record => {
          if (record.get("db") == null) throw new InvalidArgumentsException("Database '" + dbName + "' does not exist.")
        }
      )

    // STOP DATABASE foo
    case StopDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("StopDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d:Database {name: $name})
          |OPTIONAL MATCH (d2:Database {name: $name, status: $oldStatus})
          |SET d2.status = $status
          |SET d2.stopped_at = datetime()
          |RETURN d2.name as name, d2.status as status, d.name as db""".stripMargin,
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.stringValue(dbName),
            DatabaseStatus.Online,
            DatabaseStatus.Offline
          )
        ),
        record => {
          if (record.get("db") == null) throw new InvalidArgumentsException("Database '" + dbName + "' does not exist.")
        }
      )*/
  }

  override def isApplicableManagementCommand(logicalPlanState: LogicalPlanState): Boolean =
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable).isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
