/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import com.neo4j.causalclustering.core.consensus.RaftMachine
import com.neo4j.configuration.EnterpriseEditionSettings
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager
import com.neo4j.server.security.enterprise.auth.Resource
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.helpers.DatabaseNameValidator
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.DatabaseResource
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.GraphOrDatabaseScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelResource
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PropertyResource
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.CopyRolePrivileges
import org.neo4j.cypher.internal.logical.plans.CreateDatabase
import org.neo4j.cypher.internal.logical.plans.CreateRole
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.DenyDatabaseAction
import org.neo4j.cypher.internal.logical.plans.DenyDbmsAction
import org.neo4j.cypher.internal.logical.plans.DenyGraphAction
import org.neo4j.cypher.internal.logical.plans.DropDatabase
import org.neo4j.cypher.internal.logical.plans.DropRole
import org.neo4j.cypher.internal.logical.plans.EnsureValidNonSystemDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureValidNumberOfDatabases
import org.neo4j.cypher.internal.logical.plans.GrantDatabaseAction
import org.neo4j.cypher.internal.logical.plans.GrantDbmsAction
import org.neo4j.cypher.internal.logical.plans.GrantGraphAction
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.logical.plans.RequireRole
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeGraphAction
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.ShowPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowRoles
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.StartDatabase
import org.neo4j.cypher.internal.logical.plans.StopDatabase
import org.neo4j.cypher.internal.procs.ActionMapper
import org.neo4j.cypher.internal.procs.LoggingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.slottedParameters
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.dbms.api.DatabaseExistsException
import org.neo4j.dbms.api.DatabaseLimitReachedException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.impl.store.format.standard.Standard
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.StringArray
import org.neo4j.values.storable.TextArray
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.asScalaSetConverter
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * This runtime takes on queries that work on the system database, such as multidatabase and security administration commands.
 * The planning requirements for these are much simpler than normal Cypher commands, and as such the runtime stack is also different.
 */
case class EnterpriseAdministrationCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver) extends AdministrationCommandRuntime {
  private val communityCommandRuntime: CommunityAdministrationCommandRuntime = CommunityAdministrationCommandRuntime(normalExecutionEngine, resolver, logicalToExecutable)
  private val config: Config = resolver.resolveDependency(classOf[Config])
  private val maxDBLimit: Long = config.get(EnterpriseEditionSettings.max_number_of_databases)
  private val create_drop_database_is_blocked = config.get(GraphDatabaseInternalSettings.block_create_drop_database)
  private val start_stop_database_is_blocked = config.get(GraphDatabaseInternalSettings.block_start_stop_database)

  override def name: String = "enterprise administration-commands"

  private def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a recognized database administration command: ${unknownPlan.getClass.getSimpleName}")
  }

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext): ExecutionPlan = {

    val (planWithSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we delegate to communitys version of it (which supports common things like procedures)
    // If neither we throw an error
    fullLogicalToExecutable.applyOrElse(planWithSlottedParameters, throwCantCompile).apply(context, parameterMapping)
  }

  private lazy val authManager = {
    resolver.resolveDependency(classOf[EnterpriseAuthManager])
  }

  private lazy val enterpriseSecurityGraphComponent: EnterpriseSecurityGraphComponent = {
    resolver.resolveDependency(classOf[EnterpriseSecurityGraphComponent])
  }

  // This allows both community and enterprise commands to be considered together, and chained together
  private def fullLogicalToExecutable = logicalToExecutable orElse communityCommandRuntime.logicalToExecutable

  private def logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] = {
    // SHOW USERS
    case ShowUsers(source, symbols, yields, where, returns) => (context, parameterMapping) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        s"""MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |WITH u, r.name as roleNames ORDER BY roleNames
          |WITH u, collect(roleNames) as roles
          |WITH u.name as user, roles + 'PUBLIC' as roles, u.passwordChangeRequired AS passwordChangeRequired, u.suspended AS suspended
          ${AdministrationShowCommandUtils.generateWhereClause(where)}
          ${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user"))}
          |""".stripMargin,
        VirtualValues.EMPTY_MAP, source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET PASSWORD password
    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET PASSWORD $password
    case CreateUser(source, userName, password, requirePasswordChange, suspendedOptional) => (context, parameterMapping) =>
      val suspended = suspendedOptional.getOrElse(false)
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      makeCreateUserExecutionPlan(userName, password, requirePasswordChange, suspended)(sourcePlan, normalExecutionEngine)

    // ALTER USER foo [SET PASSWORD pw] [CHANGE [NOT] REQUIRED] [SET STATUS ACTIVE]
    case AlterUser(source, userName, password, requirePasswordChange, suspended) => (context, parameterMapping) =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      makeAlterUserExecutionPlan(userName, password, requirePasswordChange, suspended)(sourcePlan, normalExecutionEngine)

    // SHOW [ ALL | POPULATED ] ROLES [ WITH USERS ]
    case ShowRoles(source, withUsers, showAll, symbols, yields, where, returns) => (context, parameterMapping) =>
      val userWithColumns = if (withUsers) ", u.name as member" else ""
      val userReturnColumns = if (withUsers) ", member" else ""
      val maybeMatchUsers = if (withUsers) "OPTIONAL MATCH (u:User)" else ""
      val roleMatch = (showAll, withUsers) match {
        case (true, true) =>
          "MATCH (r:Role) WHERE r.name <> 'PUBLIC' OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r)"
        case (true, false) =>
          "MATCH (r:Role) WHERE r.name <> 'PUBLIC'"
        case (false, _) =>
          "MATCH (r:Role)<-[:HAS_ROLE]-(u:User)"
      }
      SystemCommandExecutionPlan("ShowRoles", normalExecutionEngine,
        s"""
           |CALL {
           |$roleMatch
           |
           |WITH DISTINCT r.name as role $userWithColumns
           |${AdministrationShowCommandUtils.generateWhereClause(where)}
           |RETURN role $userReturnColumns
           |UNION
           |MATCH (r:Role) WHERE r.name = 'PUBLIC'
           |$maybeMatchUsers
           |WITH DISTINCT r.name as role $userWithColumns
           |${AdministrationShowCommandUtils.generateWhereClause(where)}
           |RETURN role $userReturnColumns
           |}
           |${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("role"))}
        """.stripMargin,
        VirtualValues.EMPTY_MAP,
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS] AS COPY OF bar
    case CreateRole(source, roleName) => (context, parameterMapping) =>
      val roleNameFields = getNameFields("rolename", roleName)
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        s"""CREATE (new:Role {name: $$`${roleNameFields.nameKey}`})
          |RETURN new.name
        """.stripMargin,
        VirtualValues.map(Array(roleNameFields.nameKey), Array(roleNameFields.nameValue)),
        QueryHandler
          .handleNoResult(p => Some(new IllegalStateException(s"Failed to create the specified role '${runtimeValue(roleName, p)}'.")))
          .handleError((error, p) => (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new InvalidArgumentsException(s"Failed to create the specified role '${runtimeValue(roleName, p)}': Role already exists.", error)
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create the specified role '${runtimeValue(roleName, p)}': $followerError", error)
            case _ => new IllegalStateException(s"Failed to create the specified role '${runtimeValue(roleName, p)}'.", error)
          }),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        initFunction = (params, _) => {
          val name = runtimeValue(roleName, params)
          NameValidator.assertValidRoleName(name)
          NameValidator.assertUnreservedRoleName("create", name)
        },
        parameterConverter = roleNameFields.nameConverter
      )

    // Used to split the requirement from the source role before copying privileges
    case RequireRole(source, roleName) => (context, parameterMapping) =>
      val roleNameFields = getNameFields("rolename", roleName)
      UpdatingSystemCommandExecutionPlan("RequireRole", normalExecutionEngine,
        s"""MATCH (role:Role {name: $$`${roleNameFields.nameKey}`})
          |RETURN role.name""".stripMargin,
        VirtualValues.map(Array(roleNameFields.nameKey), Array(roleNameFields.nameValue)),
        QueryHandler
          .handleNoResult(p => Some(new InvalidArgumentsException(s"Failed to create a role as copy of '${runtimeValue(roleName, p)}': Role does not exist.")))
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create a role as copy of '${runtimeValue(roleName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to create a role as copy of '${runtimeValue(roleName, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = roleNameFields.nameConverter
      )

    // COPY PRIVILEGES FROM role1 TO role2
    case CopyRolePrivileges(source, to, from, grantDeny) => (context, parameterMapping) =>
      val toNameFields = getNameFields("toRole", to)
      val fromNameFields = getNameFields("fromRole", from)
      val mapValueConverter: (Transaction, MapValue) => MapValue = (tx, p) => toNameFields.nameConverter(tx, fromNameFields.nameConverter(tx, p))
      // This operator expects CreateRole(to) and RequireRole(from) to run as source, so we do not check for those
      UpdatingSystemCommandExecutionPlan("CopyPrivileges", normalExecutionEngine,
        s"""MATCH (to:Role {name: $$`${toNameFields.nameKey}`})
           |MATCH (from:Role {name: $$`${fromNameFields.nameKey}`})-[:$grantDeny]->(p:Privilege)
           |MERGE (to)-[g:$grantDeny]->(p)
           |RETURN from.name, to.name, count(g)""".stripMargin,
        VirtualValues.map(Array(fromNameFields.nameKey, toNameFields.nameKey), Array(fromNameFields.nameValue, toNameFields.nameValue)),
        QueryHandler.handleError((e, p) => new IllegalStateException(s"Failed to create role '${runtimeValue(to, p)}' as copy of '${runtimeValue(from, p)}': Failed to copy privileges.", e)),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = mapValueConverter
      )

    // DROP ROLE foo [IF EXISTS]
    case DropRole(source, roleName) => (context, parameterMapping) =>
      val roleNameFields = getNameFields("rolename", roleName)
      UpdatingSystemCommandExecutionPlan("DropRole", normalExecutionEngine,
        s"""MATCH (role:Role {name: $$`${roleNameFields.nameKey}`}) DETACH DELETE role
          |RETURN 1 AS ignore""".stripMargin,
        VirtualValues.map(Array(roleNameFields.nameKey), Array(roleNameFields.nameValue)),
        QueryHandler
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified role '${runtimeValue(roleName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to delete the specified role '${runtimeValue(roleName, p)}'.", error)
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        initFunction = (params, _) => NameValidator.assertUnreservedRoleName("delete", runtimeValue(roleName, params)),
        parameterConverter = roleNameFields.nameConverter
      )

    // GRANT ROLE foo TO user
    case GrantRoleToUser(source, roleName, userName) => (context, parameterMapping) =>
      val roleNameFields = getNameFields("role", roleName)
      val userNameFields = getNameFields("user", userName)
      UpdatingSystemCommandExecutionPlan("GrantRoleToUser", normalExecutionEngine,
        s"""MATCH (r:Role {name: $$`${roleNameFields.nameKey}`})
          |OPTIONAL MATCH (u:User {name: $$`${userNameFields.nameKey}`})
          |WITH r, u
          |MERGE (u)-[a:HAS_ROLE]->(r)
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array(roleNameFields.nameKey, userNameFields.nameKey), Array(roleNameFields.nameValue, userNameFields.nameValue)),
        QueryHandler
          .handleNoResult(p => Some(new InvalidArgumentsException(s"Failed to grant role '${runtimeValue(roleName, p)}' to user '${runtimeValue(userName, p)}': Role does not exist.")))
          .handleError {
            case (e: InternalException, p) if e.getMessage.contains("ignore rows where a relationship node is missing") =>
              new InvalidArgumentsException(s"Failed to grant role '${runtimeValue(roleName, p)}' to user '${runtimeValue(userName, p)}': User does not exist.", e)
            case (e: HasStatus, p) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to grant role '${runtimeValue(roleName, p)}' to user '${runtimeValue(userName, p)}': $followerError", e)
            case (e, p) => new IllegalStateException(s"Failed to grant role '${runtimeValue(roleName, p)}' to user '${runtimeValue(userName, p)}'.", e)
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        initFunction = (p, _) => {
          runtimeValue(roleName, p) != "PUBLIC"
        },
        parameterConverter = (tx, p) => userNameFields.nameConverter(tx, roleNameFields.nameConverter(tx, p))
      )

    // REVOKE ROLE foo FROM user
    case RevokeRoleFromUser(source, roleName, userName) => (context, parameterMapping) =>
      val roleNameFields = getNameFields("role", roleName)
      val userNameFields = getNameFields("user", userName)
      UpdatingSystemCommandExecutionPlan("RevokeRoleFromUser", normalExecutionEngine,
        s"""MATCH (r:Role {name: $$`${roleNameFields.nameKey}`})
          |OPTIONAL MATCH (u:User {name: $$`${userNameFields.nameKey}`})
          |WITH r, u
          |OPTIONAL MATCH (u)-[a:HAS_ROLE]->(r)
          |DELETE a
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array(roleNameFields.nameKey, userNameFields.nameKey), Array(roleNameFields.nameValue, userNameFields.nameValue)),
        QueryHandler.handleError {
          case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to revoke role '${runtimeValue(roleName, p)}' from user '${runtimeValue(userName, p)}': $followerError", error)
          case (error, p) => new IllegalStateException(s"Failed to revoke role '${runtimeValue(roleName, p)}' from user '${runtimeValue(userName, p)}'.", error)
        },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = (tx, p) => userNameFields.nameConverter(tx, roleNameFields.nameConverter(tx, p)),
        initFunction = (params, _) => NameValidator.assertUnreservedRoleName("revoke", runtimeValue(roleName, params))
      )

    // GRANT/DENY/REVOKE _ ON DBMS TO role
    case GrantDbmsAction(source, action, roleName) => (context, parameterMapping) =>
      val dbmsAction = ActionMapper.asKernelAction(action)
      makeGrantOrDenyExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant $dbmsAction privilege to role '${runtimeValue(roleName, params)}'")

    case DenyDbmsAction(source, action, roleName) => (context, parameterMapping) =>
      val dbmsAction = ActionMapper.asKernelAction(action)
      makeGrantOrDenyExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny $dbmsAction privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeDbmsAction(source, action, roleName, revokeType) => (context, parameterMapping) =>
      val dbmsAction = ActionMapper.asKernelAction(action)
      makeRevokeExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke $dbmsAction privilege from role '${runtimeValue(roleName, params)}'")

    // GRANT/DENY/REVOKE _ ON DATABASE foo TO role
    case GrantDatabaseAction(source, action, database, qualifier, roleName) => (context, parameterMapping) =>
      val databaseAction = ActionMapper.asKernelAction(action)
      makeGrantOrDenyExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant $databaseAction privilege to role '${runtimeValue(roleName, params)}'")

    case DenyDatabaseAction(source, action, database, qualifier, roleName) => (context, parameterMapping) =>
      val databaseAction = ActionMapper.asKernelAction(action)
      makeGrantOrDenyExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny $databaseAction privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeDatabaseAction(source, action, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      val databaseAction = ActionMapper.asKernelAction(action)
      makeRevokeExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke $databaseAction privilege from role '${runtimeValue(roleName, params)}'")

    // GRANT/DENY/REVOKE _ ON GRAPH foo TO role
    case GrantGraphAction(source, action, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      val graphAction = ActionMapper.asKernelAction(action)
      val actionName = if (graphAction == PrivilegeAction.TRAVERSE) "traversal" else graphAction.toString
      makeGrantOrDenyExecutionPlan(graphAction, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant $actionName privilege to role '${runtimeValue(roleName, params)}'")

    case DenyGraphAction(source, action, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      val graphAction = ActionMapper.asKernelAction(action)
      val actionName = if (graphAction == PrivilegeAction.TRAVERSE) "traversal" else graphAction.toString
      makeGrantOrDenyExecutionPlan(graphAction, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny $actionName privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeGraphAction(source, action, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      val graphAction = ActionMapper.asKernelAction(action)
      val actionName = if (graphAction == PrivilegeAction.TRAVERSE) "traversal" else graphAction.toString
      makeRevokeExecutionPlan(graphAction, resource, database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke $actionName privilege from role '${runtimeValue(roleName, params)}'")

    // SHOW [ALL | USER user | ROLE role] PRIVILEGES
    case ShowPrivileges(source, scope, symbols, yields, where, returns) => (context, parameterMapping) =>
      val currentUserKey = internalKey("currentUser")
      val currentUserRolesKey = internalKey("currentUserRoles")
      val privilegeMatch =
        """
          |MATCH (r)-[g]->(p:Privilege)-[:SCOPE]->(s:Segment),
          |    (p)-[:APPLIES_TO]->(res:Resource),
          |    (s)-[:FOR]->(d),
          |    (s)-[:QUALIFIED]->(q)
          |WHERE d:Database OR d:DatabaseAll OR d:DatabaseDefault
        """.stripMargin
      val segmentColumn =
        """
          |CASE q.type
          |  WHEN 'node' THEN 'NODE('+q.label+')'
          |  WHEN 'relationship' THEN 'RELATIONSHIP('+q.label+')'
          |  WHEN 'database' THEN 'database'
          |  WHEN 'user' THEN 'USER('+q.label+')'
          |  ELSE 'ELEMENT('+q.label+')'
          |END
        """.stripMargin
      val resourceColumn =
        """
          |CASE res.type
          |  WHEN 'property' THEN 'property('+res.arg1+')'
          |  WHEN 'label' THEN 'label('+res.arg1+')'
          |  ELSE res.type
          |END
        """.stripMargin
      val projection =
        s"""
           |WITH type(g) AS access, p.action AS action, $resourceColumn AS resource,
           |coalesce(d.name, '*') AS graph, segment, r.name AS role
        """.stripMargin

      val filtering = AdministrationShowCommandUtils.generateWhereClause(where)
      val returnClause = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user", "role", "graph", "segment", "resource", "action", "access"))

      val (nameKeys: List[String], grantees: List[Value], converter: ((Transaction, MapValue) => MapValue), query) = scope match {
        case ShowRolesPrivileges(names) =>
          val nameFields = names.zipWithIndex.map { case (name, index) => getNameFields(s"role$index", name) }
          val nameKeys = nameFields.map(_.nameKey)
          val converters: Seq[(Transaction, MapValue) => MapValue] = nameFields.map(_.nameConverter)
          val nestedConverter: (Transaction, MapValue) => MapValue = (tx, params) => {
            // example for size 3: converters(2)(tx, converters(1)(tx, converters(0)(tx, params)))
            converters.foldLeft(None: Option[MapValue]) {
              case (None, c)         => Some(c(tx, params))
              case (Some(source), c) => Some(c(tx, source))
            }.getOrElse(params)
          }

          (nameKeys, nameFields.map(_.nameValue), nestedConverter,
            s"""
               |OPTIONAL MATCH (r:Role) WHERE r.name IN [$$`${nameKeys.mkString("`, $`")}`] WITH r
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r
               |$projection
               |$filtering
               |$returnClause
          """.stripMargin
          )
        case ShowUserPrivileges(Some(name)) => // SHOW USER $user PRIVILEGES
          val userNameFields = getNameFields("user", name)
          val rolesKey = internalKey("rolesKey")
          val rolesMapper: (Transaction, MapValue) => MapValue = (tx, params) => {
            val paramsWithUsername = userNameFields.nameConverter(tx, params)
            val username = paramsWithUsername.get(userNameFields.nameKey).asInstanceOf[TextValue]
            val rolesValue: TextArray = if (username == paramsWithUsername.get(currentUserKey)) {
              paramsWithUsername.get(currentUserRolesKey) match {
                case x: TextArray => x
              }
            } else {
              val userNode = tx.findNode(Label.label("User"), "name", username.stringValue())
              if (userNode == null) {
                Values.stringArray()
              } else {
                val roles = Iterables.stream(userNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_ROLE")))
                  .map[AnyRef](r => r.getEndNode.getProperty("name")).toArray.toSeq
                val rolesAsString: Seq[String] = roles.map(_.asInstanceOf[String]) :+ "PUBLIC"
                Values.stringArray(rolesAsString: _*)
              }
            }
            paramsWithUsername.updatedWith(rolesKey, rolesValue)
          }
          (List(userNameFields.nameKey), List(userNameFields.nameValue), rolesMapper,
            s"""
               |OPTIONAL MATCH (r:Role) WHERE r.name in $$`$rolesKey` WITH r
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
               |$projection, $$`${userNameFields.nameKey}` AS user
               |$filtering
               |$returnClause
          """.stripMargin
          )
        case ShowUsersPrivileges(names) => // SHOW USER $user1, user2 PRIVILEGES
          val nameFields = names.zipWithIndex.map { case (name, index) => getNameFields(s"user$index", name) }
          val nameKeys = nameFields.map(_.nameKey)
          val rolesKey = internalKey("rolesKey")
          val converter: (Transaction, MapValue) => MapValue = (tx, params) => {
            /* Example for user1 with role1 and PUBLIC, user2 with role1, role2 and PUBLIC, user3 with PUBLIC:
             * updateParamsWithRoles(user3, user3converter(updateParamsWithRoles(user2, user2converter(updateParamsWithRoles(user1, user1converter(tx, params))))))
             *
             * Which contains the parts:
             * updateParamsWithRoles(user1, user1converter(tx, params)) gives initial role list 'PUBLIC, role1'
             * updateParamsWithRoles(user2, user2converter(user1result)) updates role list to 'PUBLIC, role1, role2'
             * updateParamsWithRoles(user3, user3converter(user2result)) will not add any new roles to the role list
             * final parameters will include: user1, user2, user3 and the role list containing 'PUBLIC, role1, role2'
            */

            def updateParamsWithRoles(usernameKey: String, paramsWithUsername: MapValue): MapValue = {
              val username = paramsWithUsername.get(usernameKey).asInstanceOf[TextValue]
              // need to collect the roles for all specified users, adding new roles to the list of previously found roles
              val oldRolesValue = paramsWithUsername.get(rolesKey)
              val oldRoleSet = if (oldRolesValue == Values.NO_VALUE) {
                Set.empty[String]
              } else {
                oldRolesValue.asInstanceOf[StringArray].asObjectCopy().toSet
              }
              val rolesValue = if (username == paramsWithUsername.get(currentUserKey)) {
                paramsWithUsername.get(currentUserRolesKey) match {
                  case x: StringArray => Values.stringArray((oldRoleSet ++ x.asObjectCopy()).toSeq:_*)
                }
              } else {
                val userNode = tx.findNode(Label.label("User"), "name", username.stringValue())
                if (userNode == null) {
                  oldRolesValue
                } else {
                  val roles = Iterables.stream(userNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_ROLE")))
                    .map[AnyRef](r => r.getEndNode.getProperty("name")).toArray.toSeq
                  val rolesAsString: Seq[String] = roles.map(_.asInstanceOf[String]) :+ "PUBLIC"
                  Values.stringArray((oldRoleSet ++ rolesAsString).toSeq: _*)
                }
              }
              paramsWithUsername.updatedWith(rolesKey, rolesValue)
            }

            nameFields.foldLeft(None: Option[MapValue]) {
              case (None, NameFields(usernameKey, _, usernameConverter))         => Some(updateParamsWithRoles(usernameKey, usernameConverter(tx, params)))
              case (Some(source), NameFields(usernameKey, _, usernameConverter)) => Some(updateParamsWithRoles(usernameKey, usernameConverter(tx, source)))
            }.getOrElse(params)
          }
          (nameKeys, nameFields.map(_.nameValue), converter,
            s"""
               |OPTIONAL MATCH (r:Role) WHERE r.name in $$`$rolesKey` WITH r
               |MATCH (u:User) WHERE u.name IN [$$`${nameKeys.mkString("`, $`")}`] AND (r.name = 'PUBLIC' OR (u)-[:HAS_ROLE]->(r)) WITH r, u
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, r.name, u.name, segment
               |$projection, u.name AS user
               |$filtering
               |$returnClause
          """.stripMargin
          )
        case ShowUserPrivileges(None) =>
          (List("grantee"), List(Values.NO_VALUE), IdentityConverter, // will generate correct parameter name later and don't want to risk clash
            s"""
               |OPTIONAL MATCH (r:Role) WHERE r.name in $$`$currentUserRolesKey` WITH r
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
               |$projection, $$`$currentUserKey` AS user
               |$filtering
               |$returnClause
          """.stripMargin
          )
        case ShowAllPrivileges() => (List("grantee"), List(Values.NO_VALUE), IdentityConverter,
          s"""
             |OPTIONAL MATCH (r:Role) WITH r
             |$privilegeMatch
             |WITH g, p, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
             |$projection
             |$filtering
             |$returnClause
          """.stripMargin
        )
        case _ => throw new IllegalStateException(s"Invalid show privilege scope '$scope'")
      }
      SystemCommandExecutionPlan("ShowPrivileges", normalExecutionEngine, query, VirtualValues.map(nameKeys.toArray, grantees.toArray),
        source = source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)),
        parameterGenerator = (_, securityContext) => VirtualValues.map(
          Array(currentUserKey, currentUserRolesKey),
          Array(Values.utf8Value(securityContext.subject().username()), Values.stringArray(securityContext.roles().asScala.toArray: _*))),
        parameterConverter = converter)

    // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
    case CreateDatabase(source, dbName) => (context, parameterMapping) =>
      if (create_drop_database_is_blocked) {
        throw new UnsupportedOperationException("CREATE DATABASE is not supported, for more info see https://aura.support.neo4j.com/hc/en-us/articles/360050567093")
      }

      // Ensuring we don't exceed the max number of databases is a separate step
      val nameFields = getNameFields("databaseName", dbName, valueMapper = s => {
        val normalizedName = new NormalizedDatabaseName(s)
        try {
          DatabaseNameValidator.validateExternalDatabaseName(normalizedName)
        } catch {
          case e: IllegalArgumentException => throw new InvalidArgumentException(e.getMessage)
        }
        normalizedName.name()
      })
      val defaultDbName = config.get(GraphDatabaseSettings.default_database)
      def isDefault(dbName: String) = dbName.equals(defaultDbName)

      val virtualKeys: Array[String] = Array(
        "status",
        "default",
        "uuid")
      def virtualValues(params: MapValue): Array[AnyValue] = Array(
        params.get(nameFields.nameKey),
        DatabaseStatus.Online,
        Values.booleanValue(isDefault(params.get(nameFields.nameKey).asInstanceOf[TextValue].stringValue())),
        Values.utf8Value(UUID.randomUUID().toString))

      def asInternalKeys(keys: Array[String]): Array[String] = keys.map(internalKey)

      def virtualMapClusterConverter(raftMachine: RaftMachine): (Transaction, MapValue) => MapValue = (_ ,params) => {
        val initialMembersSet = raftMachine.coreState.committed.members.asScala
        val initial = initialMembersSet.map(_.getUuid.toString).toArray
        val creation = System.currentTimeMillis()
        val randomId = ThreadLocalRandom.current().nextLong()
        val storeVersion = Standard.LATEST_STORE_VERSION
        VirtualValues.map(
          nameFields.nameKey +: asInternalKeys(virtualKeys ++ Array("initialMembers", "creationTime", "randomId", "storeVersion")),
          virtualValues(params) ++ Array(Values.stringArray(initial: _*), Values.longValue(creation), Values.longValue(randomId), Values.utf8Value(storeVersion)))
      }

      def virtualMapStandaloneConverter(): (Transaction, MapValue) => MapValue = (_, params) => {
        VirtualValues.map(nameFields.nameKey +: asInternalKeys(virtualKeys), virtualValues(params))
      }

      val clusterProperties =
        s"""
          |  , // needed since it might be empty string instead
          |  d.initial_members = $$`${internalKey("initialMembers")}`,
          |  d.store_creation_time = $$`${internalKey("creationTime")}`,
          |  d.store_random_id = $$`${internalKey("randomId")}`,
          |  d.store_version = $$`${internalKey("storeVersion")}` """.stripMargin

      val (queryAdditions, virtualMapConverter) = Try(resolver.resolveDependency(classOf[RaftMachine])) match {
        case Success(raftMachine) =>
          (clusterProperties, virtualMapClusterConverter(raftMachine))

        case Failure(_) => ("", virtualMapStandaloneConverter())
      }

      UpdatingSystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        s"""CREATE (d:Database {name: $$`${nameFields.nameKey}`})
           |SET
           |  d.status = $$`${internalKey("status")}`,
           |  d.default = $$`${internalKey("default")}`,
           |  d.created_at = datetime(),
           |  d.uuid = $$`${internalKey("uuid")}`
           |  $queryAdditions
           |RETURN d.name as name, d.status as status, d.uuid as uuid
        """.stripMargin, VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
        QueryHandler
          .handleError((error, params) => (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new DatabaseExistsException(s"Failed to create the specified database '${runtimeValue(dbName, params)}': Database already exists.", error)
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create the specified database '${runtimeValue(dbName, params)}': $followerError", error)
            case _ => new IllegalStateException(s"Failed to create the specified database '${runtimeValue(dbName, params)}'.", error)
          }),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = (tx, m) => virtualMapConverter(tx, nameFields.nameConverter(tx, m))
      )

    // Used to ensure we don't create to many databases,
    // this by first creating/replacing (source) and then check we didn't exceed the allowed number
    case EnsureValidNumberOfDatabases(source) =>  (context, parameterMapping) =>
      val dbName = source.databaseName // database to be created, needed for error message
      val query =
        """MATCH (d:Database)
          |RETURN count(d) as numberOfDatabases
        """.stripMargin
      UpdatingSystemCommandExecutionPlan("EnsureValidNumberOfDatabases", normalExecutionEngine, query, VirtualValues.EMPTY_MAP,
        QueryHandler.handleResult((_, numberOfDatabases, params) =>
          if (numberOfDatabases.asInstanceOf[LongValue].longValue() > maxDBLimit) {
            Some(new DatabaseLimitReachedException(s"Failed to create the specified database '${runtimeValue(dbName, params)}':"))
          } else {
            None
          }
        ),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // DROP DATABASE foo [IF EXISTS] [DESTROY | DUMP DATA]
    case DropDatabase(source, dbName, additionalAction) => (context, parameterMapping) =>
      if (create_drop_database_is_blocked) {
        throw new UnsupportedOperationException("DROP DATABASE is not supported, for more info see https://aura.support.neo4j.com/hc/en-us/articles/360050567093")
      }
      val dumpDataKey = internalKey("dumpData")
      val shouldDumpData = additionalAction == DumpData
      val nameFields = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        s"""OPTIONAL MATCH (d:Database {name: $$`${nameFields.nameKey}`})
          |OPTIONAL MATCH (d2:DeletedDatabase {name: $$`${nameFields.nameKey}`})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |SET d.dump_data = $$`$dumpDataKey`
          |SET d2.updated_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array(nameFields.nameKey, dumpDataKey), Array(nameFields.nameValue, Values.booleanValue(shouldDumpData))),
        QueryHandler.handleError {
          case (error: HasStatus, params) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified database '${runtimeValue(dbName, params)}': $followerError", error)
          case (error, params) => new IllegalStateException(s"Failed to delete the specified database '${runtimeValue(dbName, params)}'.", error)
        },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = nameFields.nameConverter
      )

    // START DATABASE foo
    case StartDatabase(source, dbName) => (context, parameterMapping) =>
      if (start_stop_database_is_blocked) {
        throw new UnsupportedOperationException("START DATABASE is not supported, for more info see https://aura.support.neo4j.com/hc/en-us/articles/360050567093")
      }

      val oldStatusKey = internalKey("oldStatus")
      val statusKey = internalKey("status")
      val nameFields = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("StartDatabase", normalExecutionEngine,
        s"""OPTIONAL MATCH (d:Database {name: $$`${nameFields.nameKey}`})
          |OPTIONAL MATCH (d2:Database {name: $$`${nameFields.nameKey}`, status: $$`$oldStatusKey`})
          |SET d.updated_at = datetime()
          |SET d2.status = $$`$statusKey`
          |SET d2.started_at = datetime()
          |RETURN d2.name as name, d2.status as status, d.name as db""".stripMargin,
        VirtualValues.map(
          Array(nameFields.nameKey, oldStatusKey, statusKey),
          Array(nameFields.nameValue,
            DatabaseStatus.Offline,
            DatabaseStatus.Online
          )
        ),
        QueryHandler
          .handleResult((offset, value, params) => {
            if (offset == 2 && (value eq Values.NO_VALUE)) Some(new DatabaseNotFoundException(s"Failed to start the specified database '${runtimeValue(dbName, params)}': Database does not exist."))
            else None
          })
          .handleError {
            case (error: HasStatus, params) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to start the specified database '${runtimeValue(dbName, params)}': $followerError", error)
            case (error, params) => new IllegalStateException(s"Failed to start the specified database '${runtimeValue(dbName, params)}'.", error)
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = nameFields.nameConverter
      )

    // STOP DATABASE foo
    case StopDatabase(source, dbName) => (context, parameterMapping) =>
      if (start_stop_database_is_blocked) {
        throw new UnsupportedOperationException("STOP DATABASE is not supported, for more info see https://aura.support.neo4j.com/hc/en-us/articles/360050567093")
      }

      val oldStatusKey = internalKey("oldStatus")
      val statusKey = internalKey("status")
      val nameFields = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("StopDatabase", normalExecutionEngine,
        s"""OPTIONAL MATCH (d:Database {name: $$`${nameFields.nameKey}`})
          |OPTIONAL MATCH (d2:Database {name: $$`${nameFields.nameKey}`, status: $$`$oldStatusKey`})
          |SET d.updated_at = datetime()
          |SET d2.status = $$`$statusKey`
          |SET d2.stopped_at = datetime()
          |RETURN d2.name as name, d2.status as status""".stripMargin,
        VirtualValues.map(
          Array(nameFields.nameKey, oldStatusKey, statusKey),
          Array(nameFields.nameValue,
            DatabaseStatus.Online,
            DatabaseStatus.Offline
          )
        ),
        QueryHandler.handleError {
          case (error: HasStatus, params) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to stop the specified database '${runtimeValue(dbName, params)}': $followerError", error)
          case (error, params) => new IllegalStateException(s"Failed to stop the specified database '${runtimeValue(dbName, params)}'.", error)
        },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = nameFields.nameConverter
      )

    // Used to check whether a database is present and not the system database,
    // which means it can be dropped and stopped.
    case EnsureValidNonSystemDatabase(source, dbName, action) => (context, parameterMapping) =>
      val nameFields = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("EnsureValidNonSystemDatabase", normalExecutionEngine,
        s"""MATCH (db:Database {name: $$`${nameFields.nameKey}`})
          |RETURN db.name AS name""".stripMargin,
        VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
        QueryHandler
          .handleNoResult(params => Some(new DatabaseNotFoundException(s"Failed to $action the specified database '${runtimeValue(dbName, params)}': Database does not exist.")))
          .handleResult((_, _, params) =>
            if (runtimeValue(dbName, params).equals(SYSTEM_DATABASE_NAME)) {
              Some(new DatabaseAdministrationException(s"Not allowed to $action system database."))
            } else {
              None
            })
          .handleError {
            case (error: HasStatus, params) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to $action the specified database '${runtimeValue(dbName, params)}': $followerError", error)
            case (error, params) => new IllegalStateException(s"Failed to $action the specified database '${runtimeValue(dbName, params)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = nameFields.nameConverter
      )

    // Used to log commands
    case LogSystemCommand(source, command) => (context, parameterMapping) =>
      LoggingSystemCommandExecutionPlan(
        fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping),
        command,
        (message, securityContext) => authManager.log(message, securityContext)
      )
  }

  private def getResourcePart(resource: ActionResource, startOfErrorMessage: String, grantName: String, matchOrMerge: String): (Value, Value, String) = resource match {
    case DatabaseResource() => (Values.NO_VALUE, Values.utf8Value(Resource.Type.DATABASE.toString), s"$matchOrMerge (res:Resource {type: $$`${privilegeKeys("resource")}`})")
    case PropertyResource(name) => (Values.utf8Value(name), Values.utf8Value(Resource.Type.PROPERTY.toString), s"$matchOrMerge (res:Resource {type: $$`${privilegeKeys("resource")}`, arg1: $$`${privilegeKeys("resourceValue")}`})")
    case AllPropertyResource() => (Values.NO_VALUE, Values.utf8Value(Resource.Type.ALL_PROPERTIES.toString), s"$matchOrMerge (res:Resource {type: $$`${privilegeKeys("resource")}`})") // The label is just for later printout of results
    case LabelResource(name) =>(Values.utf8Value(name), Values.utf8Value(Resource.Type.LABEL.toString), s"$matchOrMerge (res:Resource {type: $$`${privilegeKeys("resource")}`, arg1: $$`${privilegeKeys("resourceValue")}`})")
    case AllLabelResource() => (Values.NO_VALUE, Values.utf8Value(Resource.Type.ALL_LABELS.toString), s"$matchOrMerge (res:Resource {type: $$`${privilegeKeys("resource")}`})")
    case NoResource() => (Values.NO_VALUE, Values.utf8Value(Resource.Type.GRAPH.toString), s"$matchOrMerge (res:Resource {type: $$`${privilegeKeys("resource")}`})")
    case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege $grantName resource type $resource")
  }

  private def getQualifierPart(qualifier: PrivilegeQualifier, matchOrMerge: String): (String, Value, (Transaction, MapValue) =>  MapValue, String) = qualifier match {
    case AllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:DatabaseQualifier {type: 'database', label: ''})") // The label is just for later printout of results
    case AllDatabasesQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:DatabaseQualifier {type: 'database', label: ''})") // The label is just for later printout of results
    case LabelQualifier(name) => (privilegeKeys("label"), Values.utf8Value(name), IdentityConverter, matchOrMerge + s" (q:LabelQualifier {type: 'node', label: $$`${privilegeKeys("label")}`})")
    case LabelAllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:LabelQualifierAll {type: 'node', label: '*'})") // The label is just for later printout of results
    case RelationshipQualifier(name) => (privilegeKeys("label"), Values.utf8Value(name), IdentityConverter, matchOrMerge + s" (q:RelationshipQualifier {type: 'relationship', label: $$`${privilegeKeys("label")}`})")
    case RelationshipAllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:RelationshipQualifierAll {type: 'relationship', label: '*'})") // The label is just for later printout of results
    case UserAllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:UserQualifierAll {type: 'user', label: '*'})") // The label is just for later printout of results
    case UserQualifier(name) =>
      val nameFields = getNameFields("userQualifier", name)
      (nameFields.nameKey, nameFields.nameValue, nameFields.nameConverter, matchOrMerge + s" (q:UserQualifier {type: 'user', label: $$`${nameFields.nameKey}`})")
  }

  private def escapeName(name: Either[String, AnyRef]): String = name match {
    case Left(s) => ExpressionStringifier.backtick(s)
    case Right(p) if p.isInstanceOf[ParameterFromSlot]=> s"$$`${p.asInstanceOf[ParameterFromSlot].name}`"
    case Right(p) if p.isInstanceOf[Parameter]=> s"$$`${p.asInstanceOf[Parameter].name}`"
  }

  private val privilegeKeys = Seq("action", "resourceValue", "resource", "label").foldLeft(Map.empty[String, String])((a, k) => a + (k -> internalKey(k)))
  private def makeGrantOrDenyExecutionPlan(privilegeAction: PrivilegeAction,
                                           resource: ActionResource,
                                           database: GraphOrDatabaseScope,
                                           qualifier: PrivilegeQualifier,
                                           roleName: Either[String, Parameter],
                                           source: Option[ExecutionPlan],
                                           grant: GrantOrDeny,
                                           startOfErrorMessage: MapValue => String): UpdatingSystemCommandExecutionPlan = {

    val commandName = if (grant.isGrant) "GrantPrivilege" else "DenyPrivilege"

    val action = Values.utf8Value(privilegeAction.toString)
    val nameFields = getNameFields("role", roleName)
    val roleMap = VirtualValues.map(Array(nameFields.nameKey), Array(Values.utf8Value(escapeName(roleName))))
    val (resourceValue: Value, resourceType: Value, resourceMerge: String) = getResourcePart(resource, startOfErrorMessage(roleMap), grant.name, "MERGE")
    val (qualifierKey, qualifierValue, qualifierConverter, qualifierMerge) = getQualifierPart(qualifier, "MERGE")
    val (databaseKey, databaseValue, databaseConverter, databaseMerge, scopeMerge, specialDatabase) = database match {
      case NamedGraphScope(name) =>
        val nameFields = getNameFields("nameScope", name, valueMapper = s => new NormalizedDatabaseName(s).name())
        (nameFields.nameKey, nameFields.nameValue, nameFields.nameConverter, s"MATCH (d:Database {name: $$`${nameFields.nameKey}`})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", null)
      case NamedDatabaseScope(name) =>
        val nameFields = getNameFields("nameScope", name, valueMapper = s => new NormalizedDatabaseName(s).name())
        (nameFields.nameKey, nameFields.nameValue, nameFields.nameConverter, s"MATCH (d:Database {name: $$`${nameFields.nameKey}`})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", null)
      // Currently graph and database is considered same internally
      case AllGraphsScope() | AllDatabasesScope() => ("*", Values.utf8Value("*"), IdentityConverter, "MERGE (d:DatabaseAll {name: '*'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", SpecialDatabase.ALL) // The name is just for later printout of results
      case DefaultGraphScope() | DefaultDatabaseScope() => ("DEFAULT_DATABASE", Values.utf8Value("DEFAULT DATABASE"), IdentityConverter, "MERGE (d:DatabaseDefault {name: 'DEFAULT'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", SpecialDatabase.DEFAULT) // The name is just for later printout of results
    }
    UpdatingSystemCommandExecutionPlan(commandName, normalExecutionEngine,
      s"""
         |// Find or create the segment scope qualifier (eg. label qualifier, or all labels)
         |$qualifierMerge
         |WITH q
         |
         |// Find the specified database or find/create the special default database or the special DatabaseAll node for '*'
         |$databaseMerge
         |WITH q, d
         |
         |// Create a new scope connecting the database to the qualifier using a :Segment node
         |$scopeMerge
         |
         |// Find or create the appropriate resource type (eg. 'graph') and then connect it to the scope through a :Privilege
         |$resourceMerge
         |MERGE (res)<-[:APPLIES_TO]-(p:Privilege {action: $$`${privilegeKeys("action")}`})-[:SCOPE]->(s)
         |WITH q, d, p
         |
         |// Connect the role to the action to complete the privilege assignment
         |OPTIONAL MATCH (r:Role {name: $$`${nameFields.nameKey}`})
         |MERGE (r)-[:${grant.relType}]->(p)
         |
         |// Return the table of results
         |RETURN '${grant.prefix}' AS grant, p.action AS action, d.name AS database, q.label AS label, r.name AS role""".stripMargin,
      VirtualValues.map(Array(privilegeKeys("action"), privilegeKeys("resource"), privilegeKeys("resourceValue"), databaseKey, qualifierKey, nameFields.nameKey),
        Array(action, resourceType, resourceValue, databaseValue, qualifierValue, nameFields.nameValue)),
      QueryHandler
        .handleNoResult(params => {
          val db = params.get(databaseKey).asInstanceOf[TextValue].stringValue()
          if (db.equals("*") && !databaseKey.equals("*"))
            Some(new InvalidArgumentsException(s"${startOfErrorMessage(params)}: Parameterized database and graph names do not support wildcards."))
          else
            // needs to have the database name here since it is not in the startOfErrorMessage
            Some(new DatabaseNotFoundException(s"${startOfErrorMessage(params)}: Database '$db' does not exist."))
        })
        .handleError {
          case (e: InternalException, p) if e.getMessage.contains("ignore rows where a relationship node is missing") =>
            new InvalidArgumentsException(s"${startOfErrorMessage(p)}: Role does not exist.", e) // the rolename is included in the startOfErrorMessage so not needed here (consistent with the other commands)
          case (e: HasStatus, p) if e.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"${startOfErrorMessage(p)}: $followerError", e)
          case (e, p) => new IllegalStateException(s"${startOfErrorMessage(p)}.", e)
        },
      source,
      parameterConverter = (tx, p) => databaseConverter(tx, qualifierConverter(tx, nameFields.nameConverter(tx, p))),
      assertPrivilegeAction = tx => enterpriseSecurityGraphComponent.assertUpdateWithAction(tx, privilegeAction, specialDatabase)
    )
  }

  private def makeRevokeExecutionPlan(privilegeAction: PrivilegeAction, resource: ActionResource, database: GraphOrDatabaseScope, qualifier: PrivilegeQualifier,
                                      roleName: Either[String, Parameter], revokeType: String, source: Option[ExecutionPlan],
                                      startOfErrorMessage: MapValue => String) = {
    val action = Values.utf8Value(privilegeAction.toString)
    val nameFields = getNameFields("role", roleName)
    val roleMap = VirtualValues.map(Array(nameFields.nameKey), Array(Values.utf8Value(escapeName(roleName))))

    val (resourceValue: Value, resourceType: Value, resourceMatch: String) = getResourcePart(resource, startOfErrorMessage(roleMap), "revoke", "MATCH")
    val (qualifierKey, qualifierValue, qualifierConverter, qualifierMatch) = getQualifierPart(qualifier, "MATCH")
    val (databaseKey, databaseValue, databaseConverter, scopeMatch, specialDatabase) = database match {
      case NamedGraphScope(name) =>
        val nameFields = getNameFields("nameScope", name, valueMapper = s => new NormalizedDatabaseName(s).name())
        (nameFields.nameKey, nameFields.nameValue, nameFields.nameConverter, s"MATCH (d:Database {name: $$`${nameFields.nameKey}`})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", null)
      case NamedDatabaseScope(name) =>
        val nameFields = getNameFields("nameScope", name, valueMapper = s => new NormalizedDatabaseName(s).name())
        (nameFields.nameKey, nameFields.nameValue, nameFields.nameConverter, s"MATCH (d:Database {name: $$`${nameFields.nameKey}`})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", null)
      case AllGraphsScope() | AllDatabasesScope() => ("", Values.NO_VALUE, IdentityConverter, "MATCH (d:DatabaseAll {name: '*'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", SpecialDatabase.ALL)
      case DefaultGraphScope() | DefaultDatabaseScope() => ("", Values.NO_VALUE, IdentityConverter, "MATCH (d:DatabaseDefault {name: 'DEFAULT'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)", SpecialDatabase.DEFAULT)
    }
    UpdatingSystemCommandExecutionPlan("RevokePrivilege", normalExecutionEngine,
      s"""
         |// Find the segment scope qualifier (eg. label qualifier, or all labels)
         |$qualifierMatch
         |
         |// Find the segment connecting the database to the qualifier
         |$scopeMatch
         |
         |// Find the action connecting the resource and segment
         |$resourceMatch
         |MATCH (res)<-[:APPLIES_TO]-(p:Privilege {action: $$`${privilegeKeys("action")}`})-[:SCOPE]->(s)
         |
         |// Find the privilege assignment connecting the role to the action
         |OPTIONAL MATCH (r:Role {name: $$`${nameFields.nameKey}`})
         |WITH p, r, d, q
         |OPTIONAL MATCH (r)-[g:$revokeType]->(p)
         |
         |// Remove the assignment
         |DELETE g
         |RETURN r.name AS role, g AS grant""".stripMargin,
      VirtualValues.map(Array(privilegeKeys("action"), privilegeKeys("resource"), privilegeKeys("resourceValue"), databaseKey, qualifierKey, nameFields.nameKey),
        Array(action, resourceType, resourceValue, databaseValue, qualifierValue, nameFields.nameValue)),
      QueryHandler.handleError {
        case (e: HasStatus, p) if e.status() == Status.Cluster.NotALeader =>
          new DatabaseAdministrationOnFollowerException(s"${startOfErrorMessage(p)}: $followerError", e)
        case (e, p) => new IllegalStateException(s"${startOfErrorMessage(p)}.", e)
      },
      source,
      parameterConverter = (tx, p) => databaseConverter(tx, qualifierConverter(tx, nameFields.nameConverter(tx, p))),
      assertPrivilegeAction = tx => enterpriseSecurityGraphComponent.assertUpdateWithAction(tx, privilegeAction, specialDatabase)
    )
  }

  override def isApplicableAdministrationCommand(logicalPlanState: LogicalPlanState): Boolean =
    fullLogicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
