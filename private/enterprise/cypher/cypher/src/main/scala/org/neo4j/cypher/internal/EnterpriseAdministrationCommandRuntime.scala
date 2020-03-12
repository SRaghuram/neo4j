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
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.AllResource
import org.neo4j.cypher.internal.ast.DatabaseResource
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
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
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.CopyRolePrivileges
import org.neo4j.cypher.internal.logical.plans.CreateDatabase
import org.neo4j.cypher.internal.logical.plans.CreateRole
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.DenyDatabaseAction
import org.neo4j.cypher.internal.logical.plans.DenyDbmsAction
import org.neo4j.cypher.internal.logical.plans.DenyMatch
import org.neo4j.cypher.internal.logical.plans.DenyRead
import org.neo4j.cypher.internal.logical.plans.DenyTraverse
import org.neo4j.cypher.internal.logical.plans.DenyWrite
import org.neo4j.cypher.internal.logical.plans.DropDatabase
import org.neo4j.cypher.internal.logical.plans.DropRole
import org.neo4j.cypher.internal.logical.plans.EnsureValidNonSystemDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureValidNumberOfDatabases
import org.neo4j.cypher.internal.logical.plans.GrantDatabaseAction
import org.neo4j.cypher.internal.logical.plans.GrantDbmsAction
import org.neo4j.cypher.internal.logical.plans.GrantMatch
import org.neo4j.cypher.internal.logical.plans.GrantRead
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.GrantTraverse
import org.neo4j.cypher.internal.logical.plans.GrantWrite
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.logical.plans.RequireRole
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeMatch
import org.neo4j.cypher.internal.logical.plans.RevokeRead
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.RevokeTraverse
import org.neo4j.cypher.internal.logical.plans.RevokeWrite
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
import org.neo4j.cypher.internal.runtime.slottedParameters
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.dbms.api.DatabaseExistsException
import org.neo4j.dbms.api.DatabaseLimitReachedException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.impl.store.format.standard.Standard
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.ByteArray
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.asScalaSetConverter
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * This runtime takes on queries that require no planning, such as multidatabase administration commands
 */
case class EnterpriseAdministrationCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver) extends AdministrationCommandRuntime {
  private val communityCommandRuntime: CommunityAdministrationCommandRuntime = CommunityAdministrationCommandRuntime(normalExecutionEngine, resolver, logicalToExecutable)
  private val maxDBLimit: Long = resolver.resolveDependency( classOf[Config] ).get(EnterpriseEditionSettings.maxNumberOfDatabases)

  override def name: String = "enterprise administration-commands"

  private def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a recognized database administration command: ${unknownPlan.getClass.getSimpleName}")
  }

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext, securityContext: SecurityContext): ExecutionPlan = {

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

    // ALTER USER foo
    case AlterUser(source, userName, password, requirePasswordChange, suspended) => (context, parameterMapping) =>
      val userNameValue = makeParameterValue(userName)
      val params: Seq[(String, String, Value, MapValue => MapValue, Option[(String, Value)])] = Seq(
        password -> "credentials",
        requirePasswordChange -> "passwordChangeRequired",
        suspended -> "suspended"
      ).flatMap { param =>
        param._1 match {
          case None => Seq.empty
          case Some(value: Boolean) => Seq((param._2, param._2, Values.booleanValue(value), identity[MapValue]_, None))
          case Some(pw:Either[Array[Byte], AnyRef]) =>
            val (key, value, pwMapper) = getPasswordFields(pw)
            val (keyBytes, valueBytes, mapperBytes) = getPasswordFields(pw, rename = s => s + "_bytes", hashPw = false)
            val mapper: MapValue => MapValue = m => pwMapper(mapperBytes(m))
            Seq((param._2, key, value, mapper, Some(keyBytes -> valueBytes)))
          case Some(p) => throw new InvalidArgumentsException(s"Invalid option type for ALTER USER, expected byte array or boolean but got: ${p.getClass.getSimpleName}")
        }
      }
      val (query, keys, values, mapper, returnItems) = params.foldLeft(("MATCH (user:User {name: $name}) WITH user, user.credentials AS oldCredentials", Array.empty[String], Array.empty[Value], identity[MapValue]_, Seq.empty[(String, Value)])) { (acc, param) =>
        val property = param._1
        val key = param._2
        val value: Value = param._3
        val values: Array[Value] = acc._3 :+ value
        val extraCredentials: Seq[(String, Value)] = param._5.toSeq
        val currentCredentials: Seq[(String, Value)] = acc._5
        val mapper: MapValue => MapValue = param._4
        val accMap: MapValue => MapValue = acc._4
        val combinedMapper: MapValue => MapValue = v => mapper(accMap(v))
        val returnItems: Seq[(String, Value)] = currentCredentials ++ extraCredentials
        (acc._1 + s" SET user.$property = $$$key", acc._2 :+ key, values, combinedMapper, returnItems)
      }
      val returnText: String = (Seq("oldCredentials") ++ returnItems.map(p => s"$$${p._1}")).mkString(", ")
      val returnKeys: Seq[String] = keys ++ returnItems.map(_._1)
      val returnVals: Seq[Value] = values.toSeq ++ returnItems.map(_._2)
      UpdatingSystemCommandExecutionPlan("AlterUser", normalExecutionEngine,
        s"$query RETURN [$returnText] AS credentials",
        VirtualValues.map(returnKeys.toArray :+ "name", returnVals.toArray :+ userNameValue),
        QueryHandler
          .handleNoResult(p => Some(new InvalidArgumentsException(s"Failed to alter the specified user '${runtimeValue(userName, p)}': User does not exist.")))
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to alter the specified user '${runtimeValue(userName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to alter the specified user '${runtimeValue(userName, p)}'.", error)
          }
          .handleResult((_, value, p) => {
            val result = value.asInstanceOf[ListValue].asArray()
            val oldCredentials = SystemGraphCredential.deserialize(result(0).asInstanceOf[TextValue].stringValue(), secureHasher)
            val maybeThrowable = password match {
              case Some(Left(password: Array[Byte])) =>
                if (oldCredentials.matchesPassword(password))
                  Some(new InvalidArgumentsException(s"Failed to alter the specified user '${runtimeValue(userName, p)}': Old password and new password cannot be the same."))
                else
                  None
              case Some(Right(_)) =>
                val userNew: Array[Byte] = result(1).asInstanceOf[ByteArray].asObjectCopy()
                if (oldCredentials.matchesPassword(userNew))
                  Some(new InvalidArgumentsException(s"Failed to alter the specified user '${runtimeValue(userName, p)}': Old password and new password cannot be the same."))
                else
                  None
              case None => None
            }
            maybeThrowable
          }),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = m => mapper(m)
      )

    // SHOW [ ALL | POPULATED ] ROLES [ WITH USERS ]
    case ShowRoles(source, withUsers, showAll) => (context, parameterMapping) =>
      val predefinedRoles = Values.stringArray(PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
        PredefinedRoles.EDITOR, PredefinedRoles.READER)
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
           |RETURN DISTINCT r.name as role,
           |CASE
           | WHEN r.name IN $$predefined THEN true
           | ELSE false
           |END as isBuiltIn
           |$userColumns
           |UNION
           |MATCH (r:Role) WHERE r.name = 'PUBLIC'
           |$maybeMatchUsers
           |RETURN DISTINCT r.name as role, true as isBuiltIn
           |$userColumns
        """.stripMargin,
        VirtualValues.map(Array("predefined"), Array(predefinedRoles)),
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS] AS COPY OF bar
    case CreateRole(source, roleName) => (context, parameterMapping) =>
      val (roleNameKey, roleNameValue, roleNameConverter) = getNameFields("rolename", roleName)
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        s"""CREATE (new:Role {name: $$$roleNameKey})
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
        s"""MATCH (role:Role {name: $$$roleNameKey})
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
        s"""MATCH (to:Role {name: $$$toNameKey})
           |MATCH (from:Role {name: $$$fromNameKey})-[:$grantDeny]->(p:Privilege)
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
        s"""MATCH (role:Role {name: $$$roleNameKey}) DETACH DELETE role
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
      val (roleNamekey, roleNameValue, roleNameConverter) = getNameFields("role", roleName)
      val (userNamekey, userNameValue, userNameConverter) = getNameFields("user", userName)
      UpdatingSystemCommandExecutionPlan("GrantRoleToUser", normalExecutionEngine,
        s"""MATCH (r:Role {name: $$$roleNamekey})
          |OPTIONAL MATCH (u:User {name: $$$userNamekey})
          |WITH r, u
          |MERGE (u)-[a:HAS_ROLE]->(r)
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array(roleNamekey, userNamekey), Array(roleNameValue, userNameValue)),
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
      val (roleNamekey, roleNameValue, roleNameConverter) = getNameFields("role", roleName)
      val (userNamekey, userNameValue, userNameConverter) = getNameFields("user", userName)
      UpdatingSystemCommandExecutionPlan("RevokeRoleFromUser", normalExecutionEngine,
        s"""MATCH (r:Role {name: $$$roleNamekey})
          |OPTIONAL MATCH (u:User {name: $$$userNamekey})
          |WITH r, u
          |OPTIONAL MATCH (u)-[a:HAS_ROLE]->(r)
          |DELETE a
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array(roleNamekey, userNamekey), Array(roleNameValue, userNameValue)),
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
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, s"Failed to grant $dbmsAction privilege to role '$roleName'")

    case DenyDbmsAction(source, action, roleName) => (context, parameterMapping) =>
      val dbmsAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, s"Failed to deny $dbmsAction privilege to role '$roleName'")

    case RevokeDbmsAction(source, action, roleName, revokeType) => (context, parameterMapping) =>
      val dbmsAction = AdminActionMapper.asKernelAction(action).toString
      makeRevokeExecutionPlan(dbmsAction, DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), s"Failed to revoke $dbmsAction privilege from role '$roleName'")

    // GRANT/DENY/REVOKE _ ON DATABASE foo TO role
    case GrantDatabaseAction(source, action, database, qualifier, roleName) => (context, parameterMapping) =>
      val databaseAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, s"Failed to grant $databaseAction privilege to role '$roleName'")

    case DenyDatabaseAction(source, action, database, qualifier, roleName) => (context, parameterMapping) =>
      val databaseAction = AdminActionMapper.asKernelAction(action).toString
      makeGrantOrDenyExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, s"Failed to deny $databaseAction privilege to role '$roleName'")

    case RevokeDatabaseAction(source, action, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      val databaseAction = AdminActionMapper.asKernelAction(action).toString
      makeRevokeExecutionPlan(databaseAction, DatabaseResource()(InputPosition.NONE), database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), s"Failed to revoke $databaseAction privilege from role '$roleName'")

    // GRANT/DENY/REVOKE TRAVERSE ON GRAPH foo NODES A (*) TO role
    case GrantTraverse(source, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.TRAVERSE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, s"Failed to grant traversal privilege to role '$roleName'")

    case DenyTraverse(source, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.TRAVERSE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, s"Failed to deny traversal privilege to role '$roleName'")

    case RevokeTraverse(source, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      makeRevokeExecutionPlan(PrivilegeAction.TRAVERSE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), s"Failed to revoke traversal privilege from role '$roleName'")

    // GRANT/DENY/REVOKE READ {prop} ON GRAPH foo NODES A (*) TO role
    case GrantRead(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.READ.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, s"Failed to grant read privilege to role '$roleName'")

    case DenyRead(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.READ.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, s"Failed to deny read privilege to role '$roleName'")

    case RevokeRead(source, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      makeRevokeExecutionPlan(PrivilegeAction.READ.toString, resource, database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), s"Failed to revoke read privilege from role '$roleName'")

    // GRANT/DENY/REVOKE MATCH {prop} ON GRAPH foo NODES A (*) TO role
    case GrantMatch(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.MATCH.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, s"Failed to grant match privilege to role '$roleName'")

    case DenyMatch(source, resource, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.MATCH.toString, resource, database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, s"Failed to deny match privilege to role '$roleName'")

    case RevokeMatch(source, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      makeRevokeExecutionPlan(PrivilegeAction.MATCH.toString, resource, database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), s"Failed to revoke match privilege from role '$roleName'")

    // GRANT/DENY/REVOKE WRITE ON GRAPH foo NODES * (*) TO role
    case GrantWrite(source, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.WRITE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), GRANT, s"Failed to grant write privilege to role '$roleName'")

    case DenyWrite(source, database, qualifier, roleName) => (context, parameterMapping) =>
      makeGrantOrDenyExecutionPlan(PrivilegeAction.WRITE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), DENY, s"Failed to deny write privilege to role '$roleName'")

    case RevokeWrite(source, database, qualifier, roleName, revokeType) => (context, parameterMapping) =>
      makeRevokeExecutionPlan(PrivilegeAction.WRITE.toString, NoResource()(InputPosition.NONE), database, qualifier, roleName, revokeType,
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)), s"Failed to revoke write privilege from role '$roleName'")

    // SHOW [ALL | USER user | ROLE role] PRIVILEGES
    case ShowPrivileges(source, scope) => (context, parameterMapping) =>
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

      val (grantee: Value, query) = scope match {
        case ShowRolePrivileges(name) => (Values.utf8Value(name),
          s"""
             |OPTIONAL MATCH (r:Role) WHERE r.name = $$grantee WITH r
             |$privilegeMatch
             |WITH g, p, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
             |$returnColumns
             |$orderBy
          """.stripMargin
        )
        case ShowUserPrivileges(name) =>
          // TODO: fix so we get the actual current user
          val requestedUser = name.getOrElse("currentUser")
          (Values.utf8Value(requestedUser),
          s"""
             |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = $$grantee WITH r, u
             |$privilegeMatch
             |WITH g, p, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, u.name, r.name, segment
             |$returnColumns, u.name AS user
             |UNION
             |MATCH (u:User) WHERE u.name = $$grantee
             |OPTIONAL MATCH (r:Role) WHERE r.name = 'PUBLIC' WITH u, r
             |$privilegeMatch
             |WITH g, p, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, r.name, segment
             |$returnColumns, u.name AS user
             |$orderBy
          """.stripMargin
        )
        case ShowAllPrivileges() => (Values.NO_VALUE,
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
      SystemCommandExecutionPlan("ShowPrivileges", normalExecutionEngine, query, VirtualValues.map(Array("grantee"), Array(grantee)),
        source = source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
    case CreateDatabase(source, normalizedName) => (context, parameterMapping) =>
      // Ensuring we don't exceed the max number of databases is a separate step
      val dbName = normalizedName.name
      val defaultDbName = resolver.resolveDependency(classOf[Config]).get(GraphDatabaseSettings.default_database)
      val default = dbName.equals(defaultDbName)

      val virtualKeys: Array[String] = Array(
        "name",
        "status",
        "default",
        "uuid")
      def virtualValues(): Array[AnyValue] = Array(
        Values.utf8Value(dbName),
        DatabaseStatus.Online,
        Values.booleanValue(default),
        Values.utf8Value(UUID.randomUUID().toString))

      def virtualMapClusterGenerator(raftMachine: RaftMachine): (Transaction, SecurityContext) => MapValue = (_, _) => {
        val initialMembersSet = raftMachine.coreState.committed.members.asScala
        val initial = initialMembersSet.map(_.getUuid.toString).toArray
        val creation = System.currentTimeMillis()
        val randomId = ThreadLocalRandom.current().nextLong()
        val storeVersion = Standard.LATEST_STORE_VERSION
        VirtualValues.map(
          virtualKeys ++ Array("initialMembers", "creationTime", "randomId", "storeVersion"),
          virtualValues() ++ Array(Values.stringArray(initial: _*), Values.longValue(creation), Values.longValue(randomId), Values.utf8Value(storeVersion)))
      }

      def virtualMapStandaloneGenerator(): (Transaction, SecurityContext) => MapValue = (_, _) => {
        VirtualValues.map(virtualKeys, virtualValues())
      }

      val clusterProperties =
        """
          |  , // needed since it might be empty string instead
          |  d.initial_members = $initialMembers,
          |  d.store_creation_time = $creationTime,
          |  d.store_random_id = $randomId,
          |  d.store_version = $storeVersion """.stripMargin

      val (queryAdditions, virtualMapGenerator) = Try(resolver.resolveDependency(classOf[RaftMachine])) match {
        case Success(raftMachine) =>
          (clusterProperties, virtualMapClusterGenerator(raftMachine))

        case Failure(_) => ("", virtualMapStandaloneGenerator())
      }

      UpdatingSystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        s"""CREATE (d:Database {name: $$name})
           |SET
           |  d.status = $$status,
           |  d.default = $$default,
           |  d.created_at = datetime(),
           |  d.uuid = $$uuid
           |  $queryAdditions
           |RETURN d.name as name, d.status as status, d.uuid as uuid
        """.stripMargin, MapValue.EMPTY,
        QueryHandler
          .handleError((error, _) => (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new DatabaseExistsException(s"Failed to create the specified database '$dbName': Database already exists.", error)
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create the specified database '$dbName': $followerError", error)
            case _ => new IllegalStateException(s"Failed to create the specified database '$dbName'.", error)
          }),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterGenerator = virtualMapGenerator
      )

    // Used to ensure we don't create to many databases,
    // this by first creating/replacing (source) and then check we didn't exceed the allowed number
    case EnsureValidNumberOfDatabases(source) =>  (context, parameterMapping) =>
      val dbName = source.normalizedName.name // database to be created, needed for error message
      val query =
        """MATCH (d:Database)
          |RETURN count(d) as numberOfDatabases
        """.stripMargin
      UpdatingSystemCommandExecutionPlan("EnsureValidNumberOfDatabases", normalExecutionEngine, query, VirtualValues.EMPTY_MAP,
        QueryHandler.handleResult((_, numberOfDatabases, _) =>
          if (numberOfDatabases.asInstanceOf[LongValue].longValue() > maxDBLimit) {
            Some(new DatabaseLimitReachedException(s"Failed to create the specified database '$dbName':"))
          } else {
            None
          }
        ),
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // DROP DATABASE foo [IF EXISTS]
    case DropDatabase(source, normalizedName) => (context, parameterMapping) =>
      val dbName = normalizedName.name
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        """MATCH (d:Database {name: $name})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.utf8Value(dbName))),
        QueryHandler.handleError {
          case (error: HasStatus, _) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified database '$dbName': $followerError", error)
          case (error, _) => new IllegalStateException(s"Failed to delete the specified database '$dbName'.", error)
        },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // START DATABASE foo
    case StartDatabase(source, normalizedName) => (context, parameterMapping) =>
      val dbName = normalizedName.name
      UpdatingSystemCommandExecutionPlan("StartDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d:Database {name: $name})
          |OPTIONAL MATCH (d2:Database {name: $name, status: $oldStatus})
          |SET d2.status = $status
          |SET d2.started_at = datetime()
          |RETURN d2.name as name, d2.status as status, d.name as db""".stripMargin,
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.utf8Value(dbName),
            DatabaseStatus.Offline,
            DatabaseStatus.Online
          )
        ),
        QueryHandler
          .handleResult((offset, value, _) => {
            if (offset == 2 && (value eq Values.NO_VALUE)) Some(new DatabaseNotFoundException(s"Failed to start the specified database '$dbName': Database does not exist."))
            else None
          })
          .handleError {
            case (error: HasStatus, _) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to start the specified database '$dbName': $followerError", error)
            case (error, _) => new IllegalStateException(s"Failed to start the specified database '$dbName'.", error)
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // STOP DATABASE foo
    case StopDatabase(source, normalizedName) => (context, parameterMapping) =>
      val dbName = normalizedName.name
      UpdatingSystemCommandExecutionPlan("StopDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d2:Database {name: $name, status: $oldStatus})
          |SET d2.status = $status
          |SET d2.stopped_at = datetime()
          |RETURN d2.name as name, d2.status as status""".stripMargin,
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.utf8Value(dbName),
            DatabaseStatus.Online,
            DatabaseStatus.Offline
          )
        ),
        QueryHandler.handleError {
          case (error: HasStatus, _) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to stop the specified database '$dbName': $followerError", error)
          case (error, _) => new IllegalStateException(s"Failed to stop the specified database '$dbName'.", error)
        },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // Used to check whether a database is present and not the system database,
    // which means it can be dropped and stopped.
    case EnsureValidNonSystemDatabase(source, normalizedName, action) => (context, parameterMapping) =>
      val dbName = normalizedName.name
      if (dbName.equals(SYSTEM_DATABASE_NAME))
        throw new DatabaseAdministrationException(s"Not allowed to $action system database.")

      UpdatingSystemCommandExecutionPlan("EnsureValidNonSystemDatabase", normalExecutionEngine,
        """MATCH (db:Database {name: $name})
          |RETURN db.name AS name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.utf8Value(dbName))),
        QueryHandler
          .handleNoResult(_ => Some(new DatabaseNotFoundException(s"Failed to $action the specified database '$dbName': Database does not exist.")))
          .handleError {
            case (error: HasStatus, _) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to $action the specified database '$dbName': $followerError", error)
            case (error, _) => new IllegalStateException(s"Failed to $action the specified database '$dbName'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
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
    case DatabaseResource() => (Values.NO_VALUE, Values.utf8Value(Resource.Type.DATABASE.toString), matchOrMerge + " (res:Resource {type: $resource})")
    case PropertyResource(name) => (Values.utf8Value(name), Values.utf8Value(Resource.Type.PROPERTY.toString), matchOrMerge + " (res:Resource {type: $resource, arg1: $property})")
    case NoResource() => (Values.NO_VALUE, Values.utf8Value(Resource.Type.GRAPH.toString), matchOrMerge + " (res:Resource {type: $resource})")
    case AllResource() => (Values.NO_VALUE, Values.utf8Value(Resource.Type.ALL_PROPERTIES.toString), matchOrMerge + " (res:Resource {type: $resource})") // The label is just for later printout of results
    case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege $grantName resource type $resource")
  }

  private def getQualifierPart(qualifier: PrivilegeQualifier, startOfErrorMessage: String, grantName: String, matchOrMerge: String): (Value, String) = qualifier match {
    case AllQualifier() => (Values.NO_VALUE, matchOrMerge + " (q:DatabaseQualifier {type: 'database', label: ''})") // The label is just for later printout of results
    case LabelQualifier(name) => (Values.utf8Value(name), matchOrMerge + " (q:LabelQualifier {type: 'node', label: $label})")
    case LabelAllQualifier() => (Values.NO_VALUE, matchOrMerge + " (q:LabelQualifierAll {type: 'node', label: '*'})") // The label is just for later printout of results
    case RelationshipQualifier(name) => (Values.utf8Value(name), matchOrMerge + " (q:RelationshipQualifier {type: 'relationship', label: $label})")
    case RelationshipAllQualifier() => (Values.NO_VALUE, matchOrMerge + " (q:RelationshipQualifierAll {type: 'relationship', label: '*'})") // The label is just for later printout of results
    case UserAllQualifier() => (Values.NO_VALUE, matchOrMerge + " (q:UserQualifierAll {type: 'user', label: '*'})") // The label is just for later printout of results
    case UserQualifier(name) => (Values.stringValue(name), matchOrMerge + " (q:UserQualifier {type: 'user', label: $label})")
    case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege $grantName qualifier $qualifier")
  }

  private def makeGrantOrDenyExecutionPlan(actionName: String,
                                           resource: ActionResource,
                                           database: GraphScope,
                                           qualifier: PrivilegeQualifier,
                                           roleName: String,
                                           source: Option[ExecutionPlan],
                                           grant: GrantOrDeny,
                                           startOfErrorMessage: String): UpdatingSystemCommandExecutionPlan = {

    val commandName = if (grant.isGrant) "GrantPrivilege" else "DenyPrivilege"

    val action = Values.utf8Value(actionName)
    val role = Values.utf8Value(roleName)
    val (property: Value, resourceType: Value, resourceMerge: String) = getResourcePart(resource, startOfErrorMessage, grant.name, "MERGE")
    val (label: Value, qualifierMerge: String) = getQualifierPart(qualifier, startOfErrorMessage, grant.name, "MERGE")
    val (dbName, db, databaseMerge, scopeMerge) = database match {
      case NamedGraphScope(name) => (Values.utf8Value(name), name, "MATCH (d:Database {name: $database})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case AllGraphsScope() => (Values.NO_VALUE, "*", "MERGE (d:DatabaseAll {name: '*'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case DefaultDatabaseScope() => (Values.NO_VALUE, "DEFAULT DATABASE", "MERGE (d:DatabaseDefault {name: 'DEFAULT'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege ${grant.name} scope database $database")
    }
    UpdatingSystemCommandExecutionPlan(commandName, normalExecutionEngine,
      s"""
         |// Find or create the segment scope qualifier (eg. label qualifier, or all labels)
         |$qualifierMerge
         |WITH q
         |
         |// Find the specified database, the default database or find/create the special DatabaseAll node for '*'
         |$databaseMerge
         |WITH q, d
         |
         |// Create a new scope connecting the database to the qualifier using a :Segment node
         |$scopeMerge
         |
         |// Find or create the appropriate resource type (eg. 'graph') and then connect it to the scope through a :Privilege
         |$resourceMerge
         |MERGE (res)<-[:APPLIES_TO]-(p:Privilege {action: $$action})-[:SCOPE]->(s)
         |WITH q, d, p
         |
         |// Connect the role to the action to complete the privilege assignment
         |OPTIONAL MATCH (r:Role {name: $$role})
         |MERGE (r)-[:${grant.relType}]->(p)
         |
         |// Return the table of results
         |RETURN '${grant.prefix}' AS grant, p.action AS action, d.name AS database, q.label AS label, r.name AS role""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler
        .handleNoResult(_ => Some(new DatabaseNotFoundException(s"$startOfErrorMessage: Database '$db' does not exist."))) // needs to have the database name here since it is not in the startOfErrorMessage
        .handleError {
          case (e: InternalException, _) if e.getMessage.contains("ignore rows where a relationship node is missing") =>
            new InvalidArgumentsException(s"$startOfErrorMessage: Role does not exist.", e) // the rolename is included in the startOfErrorMessage so not needed here (consistent with the other commands)
          case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"$startOfErrorMessage: $followerError", e)
          case (e, _) => new IllegalStateException(s"$startOfErrorMessage.", e)
        },
      source
    )
  }

  private def makeRevokeExecutionPlan(actionName: String, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier,
                                      roleName: String, revokeType: String, source: Option[ExecutionPlan], startOfErrorMessage: String) = {
    val action = Values.utf8Value(actionName)
    val role = Values.utf8Value(roleName)

    val (property: Value, resourceType: Value, resourceMatch: String) = getResourcePart(resource, startOfErrorMessage, "revoke", "MATCH")
    val (label: Value, qualifierMatch: String) = getQualifierPart(qualifier, startOfErrorMessage, "revoke", "MATCH")
    val (dbName, scopeMatch) = database match {
      case NamedGraphScope(name) => (Values.utf8Value(name), "MATCH (d:Database {name: $database})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case AllGraphsScope() => (Values.NO_VALUE, "MATCH (d:DatabaseAll {name: '*'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case DefaultDatabaseScope() => (Values.NO_VALUE, "MATCH (d:DatabaseDefault {name: 'DEFAULT'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege revoke scope database $database")
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
         |MATCH (res)<-[:APPLIES_TO]-(p:Privilege {action: $$action})-[:SCOPE]->(s)
         |
         |// Find the privilege assignment connecting the role to the action
         |OPTIONAL MATCH (r:Role {name: $$role})
         |WITH p, r, d, q
         |OPTIONAL MATCH (r)-[g:$revokeType]->(p)
         |
         |// Remove the assignment
         |DELETE g
         |RETURN r.name AS role, g AS grant""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler.handleError {
        case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
          new DatabaseAdministrationOnFollowerException(s"$startOfErrorMessage: $followerError", e)
        case (e, _) => new IllegalStateException(s"$startOfErrorMessage.", e)
      },
      source
    )
  }

  override def isApplicableAdministrationCommand(logicalPlanState: LogicalPlanState): Boolean =
    fullLogicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
