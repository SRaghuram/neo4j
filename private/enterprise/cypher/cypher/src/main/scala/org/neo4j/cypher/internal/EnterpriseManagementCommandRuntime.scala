/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import com.neo4j.server.security.enterprise.auth.CommercialAuthAndUserManager
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs.{SystemCommandExecutionPlan, UpdatingSystemCommandExecutionPlan}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.kernel.api.security.UserManager
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

  private lazy val userManager: UserManager = {
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
    case CreateUser(userName, initialPassword, requirePasswordChange, suspended) => (_, _) =>
      // TODO check so we don't log plain passwords
      val credentials = userManager.getCredentialsForPassword(UTF8.encode(initialPassword))
      val createUser = SystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
        """CREATE (u:User {name:$name,credentials:$credentials,passwordChangeRequired:$requirePasswordChange,suspended:$suspended})
          |RETURN u.name as name""".stripMargin,
        VirtualValues.map(Array("name", "credentials", "requirePasswordChange", "suspended"), Array(
          Values.stringValue(userName),
          Values.stringValue(credentials),
          Values.booleanValue(requirePasswordChange),
          Values.booleanValue(suspended)
        ))
      )
      createUser

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
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        """OPTIONAL MATCH (f:Role {name:$from})
          |// FOREACH is an IF, only create the role if the 'from' node exists
          |FOREACH (ignoreMe in CASE WHEN f IS NOT NULL THEN [1] ELSE [] END |
          | CREATE (r:Role)
          | SET r.name = $name
          |)
          |WITH f
          |// Make sure the create is eagerly done
          |OPTIONAL MATCH (r:Role {name:$name})
          |RETURN r.name as name, f.name as old""".stripMargin,
        VirtualValues.map(Array("name", "from"), Array(Values.stringValue(roleName), Values.stringValue(from))),
        record => {
          if (record.get("old") == null) throw new IllegalStateException("Cannot create role '" + roleName + "' from non-existent role '" + from + "'")
        }
      )

    // CREATE ROLE foo
    case CreateRole(roleName, _) => (_, _) =>
      SystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        """CREATE (r:Role)
          |SET r.name = $name
          |RETURN r.name as name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName)))
      )

    // DROP ROLE foo
    case DropRole(roleName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("DropRole", normalExecutionEngine,
        """OPTIONAL MATCH (r:Role {name:$name})
          |WITH r, r.name as name
          |DETACH DELETE r
          |RETURN name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        record => {
          if (record.get("name") == null) throw new IllegalStateException("Cannot drop non-existent role '" + roleName + "'")
        }
      )

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
          if (record.get("name") == null) throw new IllegalStateException("Cannot drop non-existent database '" + dbName + "'")
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
          if (record.get("db") == null) throw new IllegalStateException("Cannot start non-existent database '" + dbName + "'")
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
          if (record.get("db") == null) throw new IllegalStateException("Cannot stop non-existent database '" + dbName + "'")
        }
      )*/
  }

  override def isApplicableManagementCommand(logicalPlanState: LogicalPlanState): Boolean =
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable).isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
