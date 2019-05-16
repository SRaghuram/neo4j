/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import com.neo4j.server.security.enterprise.auth.{CommercialAuthAndUserManager, EnterpriseUserManager, Resource, ResourcePrivilege}
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs.{NoResultSystemCommandExecutionPlan, QueryHandler, SystemCommandExecutionPlan, UpdatingSystemCommandExecutionPlan}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.dbms.database.DatabaseNotFoundException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.string.UTF8
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
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
    // SHOW USERS is the only case that exists in both runtimes
    // Set up test to confirm that you get the correct thing from community
    //  enterprise already have a test
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable)
      .applyOrElse(withSlottedParameters, throwCantCompile).apply(context, parameterMapping)
  }

  private lazy val userManager: EnterpriseUserManager = {
    resolver.resolveDependency(classOf[CommercialAuthAndUserManager]).getUserManager
  }

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, Map[String, Int]) => ExecutionPlan] = {
    // SHOW USERS
    case ShowUsers() => (_, _) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |RETURN u.name as user, collect(r.name) as roles, u.passwordChangeRequired AS passwordChangeRequired, u.suspended AS suspended""".stripMargin,
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
      val predefinedRoles = Values.stringArray(PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                                               PredefinedRoles.EDITOR, PredefinedRoles.READER)
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
      val names: Array[AnyValue] = Array(Values.stringValue(from), Values.stringValue(roleName))
      UpdatingSystemCommandExecutionPlan("CopyRole", normalExecutionEngine,
        """MATCH (old:Role {name: $old})
          |CREATE (new:Role {name: $new})
          |RETURN old.name, new.name""".stripMargin,
        VirtualValues.map(Array("old", "new"), names),
        QueryHandler
          .handleNoResult(() => throw new InvalidArgumentsException(s"Cannot create role '$roleName' from non-existent role '$from'."))
          .handleError(e => throw new InvalidArgumentsException(s"The specified role '$roleName' already exists."))
      )

    // CREATE ROLE foo
    case CreateRole(roleName, _) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        """CREATE (new:Role {name: $new})
          |RETURN count(*)""".stripMargin,
        VirtualValues.map(Array("new"), Array(Values.stringValue(roleName))),
        QueryHandler
          .handleNoResult(() => throw new InvalidArgumentsException(s"Failed to create role '$roleName'."))
          .handleError(e => throw new InvalidArgumentsException(s"The specified role '$roleName' already exists."))
      )

    // DROP ROLE foo
    case DropRole(roleName) => (_, _) =>
      userManager.deleteRole(roleName)
      NoResultSystemCommandExecutionPlan()

    // GRANT ROLE foo TO user
    case GrantRolesToUsers(roleNames, userNames) => (_, _) =>
      val roles = Values.stringArray(roleNames: _*)
      val users = Values.stringArray(userNames: _*)
      SystemCommandExecutionPlan("GrantRoleToUser", normalExecutionEngine,
        """UNWIND $roles AS role
          |UNWIND $users AS user
          |MATCH (r:Role {name: role}), (u:User {name: user})
          |MERGE (u)-[a:HAS_ROLE]->(r)
          |RETURN user, collect(role) AS roles""".stripMargin,
        VirtualValues.map(Array("roles","users"), Array(roles, users))
      )

    // GRANT TRAVERSE ON GRAPH foo NODES A (*) TO role
    case GrantTraverse(database, qualifier, roleName) => (_, _) =>
      makeGrantExecutionPlan(ResourcePrivilege.Action.FIND.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName)

    // GRANT READ (prop) ON GRAPH foo NODES A (*) TO role
    case GrantRead(resource, database, qualifier, roleName) => (_, _) =>
      makeGrantExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName)

    // SHOW [ALL | USER user | ROLE role] PRIVILEGES
    case ShowPrivileges(scope) => (_, _) =>
      val (grantee: Value, mainMatch, userReturn) = scope match {
        case ast.ShowRolePrivileges(name) => (Values.stringValue(name), "OPTIONAL MATCH (r:Role) WHERE r.name = $grantee WITH r", "")
        case ast.ShowUserPrivileges(name) => (Values.stringValue(name), "OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = $grantee WITH r, u", ", u.name AS user")
        case ast.ShowAllPrivileges() => (Values.NO_VALUE, "OPTIONAL MATCH (r:Role) WITH r", "")
        case _ => throw new IllegalStateException(s"Invalid show privilege scope '$scope'")
      }
      SystemCommandExecutionPlan("ShowPrivileges", normalExecutionEngine,
        s"""$mainMatch
           |MATCH (r)-[g]->(a:Action)-[:SCOPE]->(s:Segment),
           |    (a)-[:APPLIES_TO]->(res:Resource),
           |    (s)-[:FOR]->(d),
           |    (s)-[:QUALIFIED]->(q)
           |WITH g, a, res, d, q, r ORDER BY d.name, r.name, q.label
           |RETURN type(g) AS grant, a.action AS action, res.type AS resource, coalesce(d.name, '*') AS database, collect(q.label) AS labels, r.name AS role$userReturn""".stripMargin,
        VirtualValues.map(Array("grantee"), Array(grantee)),
        e => throw new InvalidArgumentsException(s"The specified grantee '${grantee.asObject()}' does not exist.", e)
      )

    // CREATE DATABASE foo
    case CreateDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        """CREATE (d:Database {name: $name})
          |SET d.status = $status
          |Set d.default = false
          |SET d.created_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name", "status"), Array(Values.stringValue(dbName.toLowerCase), DatabaseStatus.Online)),
        e => throw new InvalidArgumentsException(s"The specified database '$dbName' already exists.", e)
      )

    // DROP DATABASE foo
    case DropDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        """MATCH (d:Database {name: $name})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(dbName.toLowerCase))),
        QueryHandler.handleNoResult(() => throw new DatabaseNotFoundException("Database '" + dbName + "' does not exist."))
      )
  }

  private def makeGrantExecutionPlan(actionName: String, resource: ast.ActionResource, database: ast.GraphScope, qualifier: ast.PrivilegeQualifier, roleName: String): UpdatingSystemCommandExecutionPlan = {
    val action = Values.stringValue(actionName)
    val role = Values.stringValue(roleName)
    val (property: Value, resourceType: Value, resourceMerge: String) = resource match {
      case ast.PropertyResource(name) => (Values.stringValue(name), Values.stringValue(Resource.Type.PROPERTY.toString), "MERGE (res:Resource {type: $resource, arg1: $property})")
      case ast.NoResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.GRAPH.toString), "MERGE (res:Resource {type: $resource})")
      case ast.AllResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.ALL_PROPERTIES.toString), "MERGE (res:Resource {type: $resource})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant resource type $resource")
    }
    val (label: Value, qualifierMerge: String) = qualifier match {
      case ast.LabelQualifier(name) => (Values.stringValue(name), "MERGE (q:LabelQualifier {label: $label})")
      case ast.AllQualifier() => (Values.NO_VALUE, "MERGE (q:LabelQualifierAll {label: '*'})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant qualifier $qualifier")
    }
    val (dbName, db, databaseMerge, scopeMerge) = database match {
      case ast.NamedGraphScope(name) => (Values.stringValue(name), name, "MATCH (d:Database {name: $database})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case ast.AllGraphsScope() => (Values.NO_VALUE, "*", "MERGE (d:DatabaseAll {name: '*'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant scope database $database")
    }
    UpdatingSystemCommandExecutionPlan("GrantTraverse", normalExecutionEngine,
      s"""
         |// Find or create the segment scope qualifier (eg. label qualifier, or all labels)
         |$qualifierMerge
         |
         |WITH q
         |// Find the specified database, or find/create the special DatabaseAll node for '*'
         |$databaseMerge
         |
         |WITH q, d
         |// Create a new scope connecting the database to the qualifier using a :Segment node
         |$scopeMerge
         |
         |// Find or create the appropriate resource type (eg. 'graph') and then connect it to the scope through an :Action
         |$resourceMerge
         |MERGE (res)<-[:APPLIES_TO]-(a:Action {action: $$action})-[:SCOPE]->(s)
         |
         |WITH q, d, a
         |// Connect the role to the action to complete the privilege assignment
         |OPTIONAL MATCH (r:Role {name: $$role})
         |MERGE (r)-[:GRANTED]->(a)
         |
         |// Return the table of results
         |RETURN 'GRANT' AS grant, a.action AS action, d.name AS database, q.label AS label, r.name AS role""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler.handleError(t => throw new InvalidArgumentsException("Role '" + roleName + "' does not exist.", t)
      ).handleNoResult(() => throw new DatabaseNotFoundException("Database '" + db + "' does not exist."))
    )
  }

  override def isApplicableManagementCommand(logicalPlanState: LogicalPlanState): Boolean =
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable).isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
