/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import com.neo4j.causalclustering.core.consensus.RaftMachine
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings
import com.neo4j.server.security.enterprise.auth.Resource
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.helpers.DatabaseNameValidator
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.DatabaseResource
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelResource
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PropertyResource
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowRolePrivileges
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
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
import org.neo4j.cypher.internal.logical.plans.DenyMatch
import org.neo4j.cypher.internal.logical.plans.DenyRead
import org.neo4j.cypher.internal.logical.plans.DenyTraverse
import org.neo4j.cypher.internal.logical.plans.DropDatabase
import org.neo4j.cypher.internal.logical.plans.DropRole
import org.neo4j.cypher.internal.logical.plans.EnsureValidNonSystemDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureValidNumberOfDatabases
import org.neo4j.cypher.internal.logical.plans.GrantDatabaseAction
import org.neo4j.cypher.internal.logical.plans.GrantDbmsAction
import org.neo4j.cypher.internal.logical.plans.GrantGraphAction
import org.neo4j.cypher.internal.logical.plans.GrantMatch
import org.neo4j.cypher.internal.logical.plans.GrantRead
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.GrantTraverse
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.logical.plans.RequireRole
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeGraphAction
import org.neo4j.cypher.internal.logical.plans.RevokeMatch
import org.neo4j.cypher.internal.logical.plans.RevokeRead
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.RevokeTraverse
import org.neo4j.cypher.internal.logical.plans.ShowPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowRoles
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.StartDatabase
import org.neo4j.cypher.internal.logical.plans.StopDatabase
import org.neo4j.cypher.internal.procs.AdminActionMapper
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
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.impl.store.format.standard.Standard
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.LongValue
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
  private val maxDBLimit: Long = resolver.resolveDependency( classOf[Config] ).get(EnterpriseEditionSettings.maxNumberOfDatabases)

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

  // This allows both community and enterprise commands to be considered together, and chained together
  private def fullLogicalToExecutable = logicalToExecutable orElse communityCommandRuntime.logicalToExecutable

  private def logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] = {
    // SHOW USERS
    case ShowUsers(source) => (context, parameterMapping) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |WITH u, r.name as roleNames ORDER BY roleNames
          |WITH u, collect(roleNames) as roles
          |RETURN u.name as user, roles + 'PUBLIC' as roles, u.passwordChangeRequired AS passwordChangeRequired, u.suspended AS suspended""".stripMargin,
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
    case ShowRoles(source, withUsers, showAll) => (context, parameterMapping) =>
      val userColumns = if (withUsers) ", u.name as member" else ""
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
           |$roleMatch
           |
           |RETURN DISTINCT r.name as role
           |$userColumns
           |UNION
           |MATCH (r:Role) WHERE r.name = 'PUBLIC'
           |$maybeMatchUsers
           |RETURN DISTINCT r.name as role
           |$userColumns
        """.stripMargin,
        VirtualValues.EMPTY_MAP,
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS] AS COPY OF bar
    case CreateRole(source, roleName) => (context, parameterMapping) =>
      val (roleNameKey, roleNameValue, roleNameConverter) = getNameFields("rolename", roleName)
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        s"""CREATE (new:Role {name: $$`$roleNameKey`})
          |RETURN new.name
        """.stripMargin,
        VirtualValues.map(Array(roleNameKey), Array(roleNameValue)),
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
        parameterConverter = roleNameConverter
      )

    // Used to split the requirement from the source role before copying privileges
    case RequireRole(source, roleName) => (context, parameterMapping) =>
      val (roleNameKey, roleNameValue, roleNameConverter) = getNameFields("rolename", roleName)
      UpdatingSystemCommandExecutionPlan("RequireRole", normalExecutionEngine,
        s"""MATCH (role:Role {name: $$`$roleNameKey`})
          |RETURN role.name""".stripMargin,
        VirtualValues.map(Array(roleNameKey), Array(roleNameValue)),
        QueryHandler
          .handleNoResult(p => Some(new InvalidArgumentsException(s"Failed to create a role as copy of '${runtimeValue(roleName, p)}': Role does not exist.")))
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create a role as copy of '${runtimeValue(roleName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to create a role as copy of '${runtimeValue(roleName, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = roleNameConverter
      )

    // COPY PRIVILEGES FROM role1 TO role2
    case CopyRolePrivileges(source, to, from, grantDeny) => (context, parameterMapping) =>
      val (toNameKey, toNameValue, toNameConverter) = getNameFields("toRole", to)
      val (fromNameKey, fromNameValue, fromNameConverter) = getNameFields("fromRole", from)
      val mapValueConverter: MapValue => MapValue = p => toNameConverter(fromNameConverter(p))
      // This operator expects CreateRole(to) and RequireRole(from) to run as source, so we do not check for those
      UpdatingSystemCommandExecutionPlan("CopyPrivileges", normalExecutionEngine,
        s"""MATCH (to:Role {name: $$`$toNameKey`})
           |MATCH (from:Role {name: $$`$fromNameKey`})-[:$grantDeny]->(p:Privilege)
           |MERGE (to)-[g:$grantDeny]->(p)
           |RETURN from.name, to.name, count(g)""".stripMargin,
        VirtualValues.map(Array(fromNameKey, toNameKey), Array(fromNameValue, toNameValue)),
        QueryHandler.handleError((e, p) => new IllegalStateException(s"Failed to create role '${runtimeValue(to, p)}' as copy of '${runtimeValue(from, p)}': Failed to copy privileges.", e)),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = mapValueConverter
      )

    // DROP ROLE foo [IF EXISTS]
    case DropRole(source, roleName) => (context, parameterMapping) =>
      val (roleNameKey, roleNameValue, roleNameConverter) = getNameFields("rolename", roleName)
      UpdatingSystemCommandExecutionPlan("DropRole", normalExecutionEngine,
        s"""MATCH (role:Role {name: $$`$roleNameKey`}) DETACH DELETE role
          |RETURN 1 AS ignore""".stripMargin,
        VirtualValues.map(Array(roleNameKey), Array(roleNameValue)),
        QueryHandler
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified role '${runtimeValue(roleName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to delete the specified role '${runtimeValue(roleName, p)}'.", error)
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        initFunction = (params, _) => NameValidator.assertUnreservedRoleName("delete", runtimeValue(roleName, params)),
        parameterConverter = roleNameConverter
      )

    // GRANT ROLE foo TO user
    case GrantRoleToUser(source, roleName, userName) => (context, parameterMapping) =>
      val (roleNameKey, roleNameValue, roleNameConverter) = getNameFields("role", roleName)
      val (userNameKey, userNameValue, userNameConverter) = getNameFields("user", userName)
      UpdatingSystemCommandExecutionPlan("GrantRoleToUser", normalExecutionEngine,
        s"""MATCH (r:Role {name: $$`$roleNameKey`})
          |OPTIONAL MATCH (u:User {name: $$`$userNameKey`})
          |WITH r, u
          |MERGE (u)-[a:HAS_ROLE]->(r)
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array(roleNameKey, userNameKey), Array(roleNameValue, userNameValue)),
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
        parameterConverter = p => userNameConverter(roleNameConverter(p))
      )

    // REVOKE ROLE foo FROM user
    case RevokeRoleFromUser(source, roleName, userName) => (context, parameterMapping) =>
      val (roleNameKey, roleNameValue, roleNameConverter) = getNameFields("role", roleName)
      val (userNameKey, userNameValue, userNameConverter) = getNameFields("user", userName)
      UpdatingSystemCommandExecutionPlan("RevokeRoleFromUser", normalExecutionEngine,
        s"""MATCH (r:Role {name: $$`$roleNameKey`})
          |OPTIONAL MATCH (u:User {name: $$`$userNameKey`})
          |WITH r, u
          |OPTIONAL MATCH (u)-[a:HAS_ROLE]->(r)
          |DELETE a
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array(roleNameKey, userNameKey), Array(roleNameValue, userNameValue)),
        QueryHandler.handleError {
          case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to revoke role '${runtimeValue(roleName, p)}' from user '${runtimeValue(userName, p)}': $followerError", error)
          case (error, p) => new IllegalStateException(s"Failed to revoke role '${runtimeValue(roleName, p)}' from user '${runtimeValue(userName, p)}'.", error)
        },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = p => userNameConverter(roleNameConverter(p)),
        initFunction = (params, _) => NameValidator.assertUnreservedRoleName("revoke", runtimeValue(roleName, params))
      )

    // GRANT/DENY/REVOKE _ ON DBMS TO role
    case GrantDbmsAction(source, action, roleName) => (context, parameterMapping) =>
      val dbmsAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant $dbmsAction privilege to role '${runtimeValue(roleName, params)}'")

    case DenyDbmsAction(source, action, roleName) => (context, parameterMapping) =>
      val dbmsAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny $dbmsAction privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeDbmsAction(source, action, roleName, revokeType) => (context, parameterMapping) =>
      val dbmsAction = AdminActionMapper.asKernelAction(action).toString
      makeRevokeExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke $dbmsAction privilege from role '${runtimeValue(roleName, params)}'")

    // GRANT/DENY/REVOKE _ ON DATABASE foo TO role
    case GrantDatabaseAction(source, action, database, qualifier, roleName) => (context, parameterMapping) =>
      val databaseAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant $databaseAction privilege to role '${runtimeValue(roleName, params)}'")

    case DenyDatabaseAction(source, action, database, qualifier, roleName) => (context, parameterMapping) =>
      val databaseAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny $databaseAction privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeDatabaseAction(source, action, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      val databaseAction = AdminActionMapper.asKernelAction(action).toString
      makeRevokeExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke $databaseAction privilege from role '${runtimeValue(roleName, params)}'")

    // GRANT/DENY/REVOKE _ ON DATABASE foo TO role
    case GrantGraphAction(source, action, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      val graphAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(graphAction, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant $graphAction privilege to role '${runtimeValue(roleName, params)}'")

    case DenyGraphAction(source, action, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      val graphAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(graphAction, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny $graphAction privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeGraphAction(source, action, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      val graphAction = AdminActionMapper.asKernelAction(action).toString
      makeRevokeExecutionPlan(graphAction, resource, database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke $graphAction privilege from role '${runtimeValue(roleName, params)}'")

    // GRANT/DENY/REVOKE TRAVERSE ON GRAPH foo NODES A (*) TO role
    case GrantTraverse(source, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.TRAVERSE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant traversal privilege to role '${runtimeValue(roleName, params)}'")

    case DenyTraverse(source, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.TRAVERSE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny traversal privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeTraverse(source, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      makeRevokeExecutionPlan(PrivilegeAction.TRAVERSE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke traversal privilege from role '${runtimeValue(roleName, params)}'")

    // GRANT/DENY/REVOKE READ {prop} ON GRAPH foo NODES A (*) TO role
    case GrantRead(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.READ.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant read privilege to role '${runtimeValue(roleName, params)}'")

    case DenyRead(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.READ.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny read privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeRead(source, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      makeRevokeExecutionPlan(PrivilegeAction.READ.toString, resource, database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke read privilege from role '${runtimeValue(roleName, params)}'")

    // GRANT/DENY/REVOKE MATCH {prop} ON GRAPH foo NODES A (*) TO role
    case GrantMatch(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.MATCH.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, params => s"Failed to grant match privilege to role '${runtimeValue(roleName, params)}'")

    case DenyMatch(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.MATCH.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, params => s"Failed to deny match privilege to role '${runtimeValue(roleName, params)}'")

    case RevokeMatch(source, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      makeRevokeExecutionPlan(PrivilegeAction.MATCH.toString, resource, database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), params => s"Failed to revoke match privilege from role '${runtimeValue(roleName, params)}'")

    // SHOW [ALL | USER user | ROLE role] PRIVILEGES
    case ShowPrivileges(source, scope) => (context, parameterMapping) =>
      val currentUserKey = internalKey("currentUser")
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
      val returnColumns =
        s"""
           |RETURN type(g) AS access, p.action AS action, $resourceColumn AS resource,
           |coalesce(d.name, '*') AS graph, segment, r.name AS role
        """.stripMargin
      val orderBy =
        s"""
           |ORDER BY role, graph, segment, resource, action
        """.stripMargin

      val (nameKey: String, grantee: Value, converter: Function[MapValue, MapValue], query) = scope match {
        case ShowRolePrivileges(name) =>
          val (roleNameKey, roleNameValue, roleNameConverter) = getNameFields("role", name)
          (roleNameKey, roleNameValue, roleNameConverter,
            s"""
               |OPTIONAL MATCH (r:Role) WHERE r.name = $$`$roleNameKey` WITH r
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
               |$returnColumns
               |$orderBy
          """.stripMargin
          )
        case ShowUserPrivileges(Some(name)) =>
          val (userNameKey, userNameValue, userNameConverter) = getNameFields("user", name)
          (userNameKey, userNameValue, userNameConverter,
            s"""
               |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = $$`$userNameKey` WITH r, u
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, u.name, r.name, segment
               |$returnColumns, u.name AS user
               |UNION
               |MATCH (u:User) WHERE u.name = $$`$userNameKey`
               |OPTIONAL MATCH (r:Role) WHERE r.name = 'PUBLIC' WITH u, r
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, r.name, segment
               |$returnColumns, u.name AS user
               |$orderBy
          """.stripMargin
          )
        case ShowUserPrivileges(None) =>
          ("grantee", Values.NO_VALUE, IdentityConverter, // will generate correct parameter name later and don't want to risk clash
            s"""
               |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = $$`$currentUserKey` WITH r, u
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, u.name, r.name, segment
               |$returnColumns, u.name AS user
               |UNION
               |MATCH (u:User) WHERE u.name = $$`$currentUserKey`
               |OPTIONAL MATCH (r:Role) WHERE r.name = 'PUBLIC' WITH u, r
               |$privilegeMatch
               |WITH g, p, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, r.name, segment
               |$returnColumns, u.name AS user
               |$orderBy
          """.stripMargin
          )
        case ShowAllPrivileges() => ("grantee", Values.NO_VALUE, IdentityConverter,
          s"""
             |OPTIONAL MATCH (r:Role) WITH r
             |$privilegeMatch
             |WITH g, p, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
             |$returnColumns
             |$orderBy
          """.stripMargin
        )
        case _ => throw new IllegalStateException(s"Invalid show privilege scope '$scope'")
      }
      SystemCommandExecutionPlan("ShowPrivileges", normalExecutionEngine, query, VirtualValues.map(Array(nameKey), Array(grantee)),
        source = source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)),
        parameterGenerator = (_, securityContext) => VirtualValues.map(Array(currentUserKey), Array(Values.utf8Value(securityContext.subject().username()))),
        parameterConverter = converter)

    // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
    case CreateDatabase(source, dbName) => (context, parameterMapping) =>
      // Ensuring we don't exceed the max number of databases is a separate step
      val (nameKey, nameValue, nameConverter) = getNameFields("databaseName", dbName, valueMapper = s => {
        val normalizedName = new NormalizedDatabaseName(s)
        try {
          DatabaseNameValidator.validateExternalDatabaseName(normalizedName)
        } catch {
          case e: IllegalArgumentException => throw new InvalidArgumentException(e.getMessage)
        }
        normalizedName.name()
      })
      val defaultDbName = resolver.resolveDependency(classOf[Config]).get(GraphDatabaseSettings.default_database)
      def isDefault(dbName: String) = dbName.equals(defaultDbName)

      val virtualKeys: Array[String] = Array(
        "status",
        "default",
        "uuid")
      def virtualValues(params: MapValue): Array[AnyValue] = Array(
        params.get(nameKey),
        DatabaseStatus.Online,
        Values.booleanValue(isDefault(params.get(nameKey).asInstanceOf[TextValue].stringValue())),
        Values.utf8Value(UUID.randomUUID().toString))

      def asInternalKeys(keys: Array[String]): Array[String] = keys.map(internalKey)

      def virtualMapClusterConverter(raftMachine: RaftMachine): MapValue => MapValue = params => {
        val initialMembersSet = raftMachine.coreState.committed.members.asScala
        val initial = initialMembersSet.map(_.getUuid.toString).toArray
        val creation = System.currentTimeMillis()
        val randomId = ThreadLocalRandom.current().nextLong()
        val storeVersion = Standard.LATEST_STORE_VERSION
        VirtualValues.map(
          nameKey +: asInternalKeys(virtualKeys ++ Array("initialMembers", "creationTime", "randomId", "storeVersion")),
          virtualValues(params) ++ Array(Values.stringArray(initial: _*), Values.longValue(creation), Values.longValue(randomId), Values.utf8Value(storeVersion)))
      }

      def virtualMapStandaloneConverter(): MapValue => MapValue = params => {
        VirtualValues.map(nameKey +: asInternalKeys(virtualKeys), virtualValues(params))
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
        s"""CREATE (d:Database {name: $$`$nameKey`})
           |SET
           |  d.status = $$`${internalKey("status")}`,
           |  d.default = $$`${internalKey("default")}`,
           |  d.created_at = datetime(),
           |  d.uuid = $$`${internalKey("uuid")}`
           |  $queryAdditions
           |RETURN d.name as name, d.status as status, d.uuid as uuid
        """.stripMargin, VirtualValues.map(Array(nameKey), Array(nameValue)),
        QueryHandler
          .handleError((error, params) => (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new DatabaseExistsException(s"Failed to create the specified database '${runtimeValue(dbName, params)}': Database already exists.", error)
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create the specified database '${runtimeValue(dbName, params)}': $followerError", error)
            case _ => new IllegalStateException(s"Failed to create the specified database '${runtimeValue(dbName, params)}'.", error)
          }),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = m => virtualMapConverter(nameConverter(m))
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

    // DROP DATABASE foo [IF EXISTS]
    case DropDatabase(source, dbName) => (context, parameterMapping) =>
      val (key, value, converter) = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        s"""MATCH (d:Database {name: $$`$key`})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array(key), Array(value)),
        QueryHandler.handleError {
          case (error: HasStatus, params) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified database '${runtimeValue(dbName, params)}': $followerError", error)
          case (error, params) => new IllegalStateException(s"Failed to delete the specified database '${runtimeValue(dbName, params)}'.", error)
        },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = converter
      )

    // START DATABASE foo
    case StartDatabase(source, dbName) => (context, parameterMapping) =>
      val oldStatusKey = internalKey("oldStatus")
      val statusKey = internalKey("status")
      val (key, value, converter) = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("StartDatabase", normalExecutionEngine,
        s"""OPTIONAL MATCH (d:Database {name: $$`$key`})
          |OPTIONAL MATCH (d2:Database {name: $$`$key`, status: $$`$oldStatusKey`})
          |SET d2.status = $$`$statusKey`
          |SET d2.started_at = datetime()
          |RETURN d2.name as name, d2.status as status, d.name as db""".stripMargin,
        VirtualValues.map(
          Array(key, oldStatusKey, statusKey),
          Array(value,
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
        parameterConverter = converter
      )

    // STOP DATABASE foo
    case StopDatabase(source, dbName) => (context, parameterMapping) =>
      val oldStatusKey = internalKey("oldStatus")
      val statusKey = internalKey("status")
      val (key, value, converter) = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("StopDatabase", normalExecutionEngine,
        s"""OPTIONAL MATCH (d2:Database {name: $$`$key`, status: $$`$oldStatusKey`})
          |SET d2.status = $$`$statusKey`
          |SET d2.stopped_at = datetime()
          |RETURN d2.name as name, d2.status as status""".stripMargin,
        VirtualValues.map(
          Array(key, oldStatusKey, statusKey),
          Array(value,
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
        parameterConverter = converter
      )

    // Used to check whether a database is present and not the system database,
    // which means it can be dropped and stopped.
    case EnsureValidNonSystemDatabase(source, dbName, action) => (context, parameterMapping) =>
      val (key, value, converter) = getNameFields("databaseName", dbName, valueMapper = s => new NormalizedDatabaseName(s).name())
      UpdatingSystemCommandExecutionPlan("EnsureValidNonSystemDatabase", normalExecutionEngine,
        s"""MATCH (db:Database {name: $$`$key`})
          |RETURN db.name AS name""".stripMargin,
        VirtualValues.map(Array(key), Array(value)),
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
        parameterConverter = converter
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

  private def getQualifierPart(qualifier: PrivilegeQualifier, startOfErrorMessage: String, grantName: String, matchOrMerge: String): (String, Value, Function[MapValue, MapValue], String) = qualifier match {
    case AllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:DatabaseQualifier {type: 'database', label: ''})") // The label is just for later printout of results
    case LabelQualifier(name) => (privilegeKeys("label"), Values.utf8Value(name), IdentityConverter, matchOrMerge + s" (q:LabelQualifier {type: 'node', label: $$`${privilegeKeys("label")}`})")
    case LabelAllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:LabelQualifierAll {type: 'node', label: '*'})") // The label is just for later printout of results
    case RelationshipQualifier(name) => (privilegeKeys("label"), Values.utf8Value(name), IdentityConverter, matchOrMerge + s" (q:RelationshipQualifier {type: 'relationship', label: $$`${privilegeKeys("label")}`})")
    case RelationshipAllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:RelationshipQualifierAll {type: 'relationship', label: '*'})") // The label is just for later printout of results
    case UserAllQualifier() => ("", Values.NO_VALUE, IdentityConverter, matchOrMerge + " (q:UserQualifierAll {type: 'user', label: '*'})") // The label is just for later printout of results
    case UserQualifier(name) =>
      val (key, value, converter) = getNameFields("userQualifier", name)
      (key, value, converter, matchOrMerge + s" (q:UserQualifier {type: 'user', label: $$`$key`})")
    case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege $grantName qualifier $qualifier")
  }

  private def escapeName(name: Either[String, AnyRef]): String = name match {
    case Left(s) => ExpressionStringifier.backtick(s)
    case Right(p) if p.isInstanceOf[ParameterFromSlot]=> s"$$`${p.asInstanceOf[ParameterFromSlot].name}`"
    case Right(p) if p.isInstanceOf[Parameter]=> s"$$`${p.asInstanceOf[Parameter].name}`"
  }

  private val privilegeKeys = Seq("action", "resourceValue", "resource", "label").foldLeft(Map.empty[String, String])((a, k) => a + (k -> internalKey(k)))
  private def makeGrantOrDenyExecutionPlan(actionName: String,
                                           resource: ActionResource,
                                           database: GraphScope,
                                           qualifier: PrivilegeQualifier,
                                           roleName: Either[String, Parameter],
                                           source: Option[ExecutionPlan],
                                           grant: GrantOrDeny,
                                           startOfErrorMessage: MapValue => String): UpdatingSystemCommandExecutionPlan = {

    val commandName = if (grant.isGrant) "GrantPrivilege" else "DenyPrivilege"

    val action = Values.utf8Value(actionName)
    val (roleKey, roleValue, roleConverter) = getNameFields("role", roleName)
    val roleMap = VirtualValues.map(Array(roleKey), Array(Values.utf8Value(escapeName(roleName))))
    val (resourceValue: Value, resourceType: Value, resourceMerge: String) = getResourcePart(resource, startOfErrorMessage(roleMap), grant.name, "MERGE")
    val (qualifierKey, qualifierValue, qualifierConverter, qualifierMerge) = getQualifierPart(qualifier, startOfErrorMessage(roleMap), grant.name, "MERGE")
    val (databaseKey, databaseValue, databaseConverter, databaseMerge, scopeMerge) = database match {
      case NamedGraphScope(name) =>
        val (key, value, converter) = getNameFields("nameScope", name, valueMapper = s => new NormalizedDatabaseName(s).name())
        (key, value, converter, s"MATCH (d:Database {name: $$`$key`})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case AllGraphsScope() => ("*", Values.utf8Value("*"), IdentityConverter, "MERGE (d:DatabaseAll {name: '*'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case DefaultDatabaseScope() => ("DEFAULT_DATABASE", Values.utf8Value("DEFAULT DATABASE"), IdentityConverter, "MERGE (d:DatabaseDefault {name: 'DEFAULT'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
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
         |OPTIONAL MATCH (r:Role {name: $$`$roleKey`})
         |MERGE (r)-[:${grant.relType}]->(p)
         |
         |// Return the table of results
         |RETURN '${grant.prefix}' AS grant, p.action AS action, d.name AS database, q.label AS label, r.name AS role""".stripMargin,
      VirtualValues.map(Array(privilegeKeys("action"), privilegeKeys("resource"), privilegeKeys("resourceValue"), databaseKey, qualifierKey, roleKey),
        Array(action, resourceType, resourceValue, databaseValue, qualifierValue, roleValue)),
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
      parameterConverter = p => databaseConverter(qualifierConverter(roleConverter(p)))
    )
  }

  private def makeRevokeExecutionPlan(actionName: String, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier,
                                      roleName: Either[String, Parameter], revokeType: String, source: Option[ExecutionPlan],
                                      startOfErrorMessage: MapValue => String) = {
    val action = Values.utf8Value(actionName)
    val (roleKey, roleValue, roleConverter) = getNameFields("role", roleName)
    val roleMap = VirtualValues.map(Array(roleKey), Array(Values.utf8Value(escapeName(roleName))))

    val (resourceValue: Value, resourceType: Value, resourceMatch: String) = getResourcePart(resource, startOfErrorMessage(roleMap), "revoke", "MATCH")
    val (qualifierKey, qualifierValue, qualifierConverter, qualifierMatch) = getQualifierPart(qualifier, startOfErrorMessage(roleMap), "revoke", "MATCH")
    val (databaseKey, databaseValue, databaseConverter, scopeMatch) = database match {
      case NamedGraphScope(name) =>
        val (key, value, converter) = getNameFields("nameScope", name, valueMapper = s => new NormalizedDatabaseName(s).name())
        (key, value, converter, s"MATCH (d:Database {name: $$`$key`})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case AllGraphsScope() => ("", Values.NO_VALUE, IdentityConverter, "MATCH (d:DatabaseAll {name: '*'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case DefaultDatabaseScope() => ("", Values.NO_VALUE, IdentityConverter, "MATCH (d:DatabaseDefault {name: 'DEFAULT'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
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
         |OPTIONAL MATCH (r:Role {name: $$`$roleKey`})
         |WITH p, r, d, q
         |OPTIONAL MATCH (r)-[g:$revokeType]->(p)
         |
         |// Remove the assignment
         |DELETE g
         |RETURN r.name AS role, g AS grant""".stripMargin,
      VirtualValues.map(Array(privilegeKeys("action"), privilegeKeys("resource"), privilegeKeys("resourceValue"), databaseKey, qualifierKey, roleKey),
        Array(action, resourceType, resourceValue, databaseValue, qualifierValue, roleValue)),
      QueryHandler.handleError {
        case (e: HasStatus, p) if e.status() == Status.Cluster.NotALeader =>
          new DatabaseAdministrationOnFollowerException(s"${startOfErrorMessage(p)}: $followerError", e)
        case (e, p) => new IllegalStateException(s"${startOfErrorMessage(p)}.", e)
      },
      source,
      parameterConverter = p => databaseConverter(qualifierConverter(roleConverter(p)))
    )
  }

  override def isApplicableAdministrationCommand(logicalPlanState: LogicalPlanState): Boolean =
    fullLogicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
