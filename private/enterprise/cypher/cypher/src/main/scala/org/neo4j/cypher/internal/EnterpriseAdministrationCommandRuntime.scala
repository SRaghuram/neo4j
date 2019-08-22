/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.util
import java.util.concurrent.ThreadLocalRandom

import com.neo4j.causalclustering.core.consensus.RaftMachine
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.{DENY, GRANT}
import com.neo4j.server.security.enterprise.auth._
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs._
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.dbms.api.{DatabaseExistsException, DatabaseLimitReachedException, DatabaseNotFoundException}
import org.neo4j.exceptions.{CantCompileQueryException, DatabaseAdministrationException, InternalException}
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.api.exceptions.{InvalidArgumentsException, Status}
import org.neo4j.kernel.impl.store.format.standard.Standard
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * This runtime takes on queries that require no planning, such as multidatabase administration commands
  */
case class EnterpriseAdministrationCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver) extends AdministrationCommandRuntime {
  private val communityCommandRuntime: CommunityAdministrationCommandRuntime = CommunityAdministrationCommandRuntime(normalExecutionEngine, resolver)
  private val maxDBLimit: Long = resolver.resolveDependency( classOf[Config] ).get(EnterpriseEditionSettings.maxNumberOfDatabases)
  private def fullLogicalToExecutable = logicalToExecutable orElse communityCommandRuntime.logicalToExecutable

  override def name: String = "enterprise administration-commands"

  private def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a recognized database administration command: ${unknownPlan.getClass.getSimpleName}")
  }

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext, securityContext: SecurityContext): ExecutionPlan = {

    val (planWithSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we delegate to communitys version of it (which supports common things like procedures)
    // If neither we throw an error
    fullLogicalToExecutable.applyOrElse(planWithSlottedParameters, throwCantCompile).apply(context, parameterMapping, securityContext)
  }

  private lazy val authManager = {
    resolver.resolveDependency(classOf[EnterpriseAuthManager])
  }

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping, SecurityContext) => ExecutionPlan] = {
    // SHOW USERS
    case ShowUsers() => (_, _, _) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |RETURN u.name as user, collect(r.name) as roles, u.passwordChangeRequired AS passwordChangeRequired, u.suspended AS suspended""".stripMargin,
        VirtualValues.EMPTY_MAP
      )

    // CREATE [OR REPLACE] USER [IF NOT EXISTS] foo SET PASSWORD password
    case CreateUser(userName, Some(initialPassword), None, requirePasswordChange, suspendedOptional, replace, allowExistingUser) => (_, _, _) =>
      val suspended = suspendedOptional.getOrElse(false)
      try {
        validatePassword(initialPassword)
        val query = if (replace) {
          """OPTIONAL MATCH (old:User {name: $name})
            |DETACH DELETE old
            |
            |CREATE (new:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})
            |RETURN new.name
          """.stripMargin
        } else if (allowExistingUser) {
          """MERGE (u:User {name: $name})
            |ON CREATE SET u.credentials = $credentials, u.passwordChangeRequired = $passwordChangeRequired, u.suspended = $suspended
            |RETURN u.name""".stripMargin
        } else {
          // NOTE: If username already exists we will violate a constraint
          """CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})
            |RETURN u.name""".stripMargin
        }

        UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine, query,
          VirtualValues.map(
            Array("name", "credentials", "passwordChangeRequired", "suspended"),
            Array(
              Values.stringValue(userName),
              Values.stringValue(authManager.createCredentialForPassword(initialPassword).serialize()),
              Values.booleanValue(requirePasswordChange),
              Values.booleanValue(suspended))),
          QueryHandler
            .handleNoResult(() => Some(new IllegalStateException(s"Failed to create the specified user '$userName'.")))
            .handleError(error => (error, error.getCause) match {
              case (_, _: UniquePropertyValueValidationException) =>
                new InvalidArgumentsException(s"Failed to create the specified user '$userName': User already exists.", error)
              case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
                new IllegalStateException(s"Failed to create the specified user '$userName': $followerError", error)
              case _ => new IllegalStateException(s"Failed to create the specified user '$userName'.", error)
            })
        )
      } finally {
        // Clear password
        if (initialPassword != null) util.Arrays.fill(initialPassword, 0.toByte)
      }

    // ALTER USER foo
    case AlterUser(userName, initialPassword, None, requirePasswordChange, suspended) => (_, _, _) =>
      val params = Seq(
        initialPassword -> "credentials",
        requirePasswordChange -> "passwordChangeRequired",
        suspended -> "suspended"
      ).flatMap { param =>
        param._1 match {
          case None => Seq.empty
          case Some(value: Boolean) => Seq((param._2, Values.booleanValue(value)))
          case Some(value: Array[Byte]) =>
            Seq((param._2, Values.stringValue(authManager.createCredentialForPassword(validatePassword(value)).serialize())))
          case Some(p) => throw new InvalidArgumentsException(s"Invalid option type for ALTER USER, expected byte array or boolean but got: ${p.getClass.getSimpleName}")
        }
      }
      val (query, keys, values) = params.foldLeft(("MATCH (user:User {name: $name}) WITH user, user.credentials AS oldCredentials", Array.empty[String], Array.empty[AnyValue])) { (acc, param) =>
        val key = param._1
        (acc._1 + s" SET user.$key = $$$key", acc._2 :+ key, acc._3 :+ param._2)
      }
      UpdatingSystemCommandExecutionPlan("AlterUser", normalExecutionEngine,
        s"$query RETURN oldCredentials",
        VirtualValues.map(keys :+ "name", values :+ Values.stringValue(userName)),
        QueryHandler
          .handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to alter the specified user '$userName': User does not exist.")))
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new IllegalStateException(s"Failed to alter the specified user '$userName': $followerError", error)
            case error => new IllegalStateException(s"Failed to alter the specified user '$userName'.", error)
          }
          .handleResult((_, value) => {
            val maybeThrowable = initialPassword match {
              case Some(password) =>
                val oldCredentials = authManager.deserialize(value.asInstanceOf[TextValue].stringValue())
                if (oldCredentials.matchesPassword(password))
                  Some(new InvalidArgumentsException(s"Failed to alter the specified user '$userName': Old password and new password cannot be the same."))
                else
                  None
              case None => None
            }
            maybeThrowable
          })
      )

    // ALTER USER foo
    case AlterUser(userName, _, Some(_), _, _) =>
      throw new IllegalStateException(s"Failed to alter the specified user '$userName': Did not resolve parameters correctly.")

    // SHOW [ ALL | POPULATED ] ROLES [ WITH USERS ]
    case ShowRoles(withUsers, showAll) => (_, _, _) =>
      val predefinedRoles = Values.stringArray(PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
        PredefinedRoles.EDITOR, PredefinedRoles.READER)
      val query = if (showAll)
        """MATCH (r:Role)
          |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r)
          |RETURN DISTINCT r.name as role,
          |CASE
          | WHEN r.name IN $predefined THEN true
          | ELSE false
          |END as isBuiltIn
        """.stripMargin
      else
        """MATCH (r:Role)<-[:HAS_ROLE]-(u:User)
          |RETURN DISTINCT r.name as role,
          |CASE
          | WHEN r.name IN $predefined THEN true
          | ELSE false
          |END as isBuiltIn
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

    // CREATE [OR REPLACE] ROLE [IF NOT EXISTS] foo AS COPY OF bar
    case CreateRole(source, roleName, replace, allowExistingRole) => (context, parameterMapping, securityContext) =>
      val query = if (replace) {
        """
          |OPTIONAL MATCH (old:Role {name: $name})
          |DETACH DELETE old
          |
          |CREATE (new:Role {name: $name, justCreated: true})
          |RETURN new.name
        """.stripMargin
      } else if (allowExistingRole) {
        """
          |MERGE (new:Role {name: $name})
          |ON CREATE SET new.justCreated = true
          |ON MATCH SET new.justCreated = false
          |RETURN new.name
        """.stripMargin
      } else {
        """
          |CREATE (new:Role {name: $name, justCreated: true})
          |RETURN new.name
        """.stripMargin
      }
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine, query,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        QueryHandler
          .handleNoResult(() => Some(new IllegalStateException(s"Failed to create the specified role '$roleName'.")))
          .handleError(error => (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new InvalidArgumentsException(s"Failed to create the specified role '$roleName': Role already exists.", error)
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new IllegalStateException(s"Failed to create the specified role '$roleName': $followerError", error)
            case _ => new IllegalStateException(s"Failed to create the specified role '$roleName'.", error)
          }),
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // Used to split the requirement from the source role before copying privileges
    case RequireRole(source, roleName) => (context, parameterMapping, securityContext) =>
      UpdatingSystemCommandExecutionPlan("RequireRole", normalExecutionEngine,
        """MATCH (role:Role {name: $name})
          |RETURN role.name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        QueryHandler.handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to create a role as copy of '$roleName': Role does not exist."))),
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // COPY PRIVILEGES FROM role1 TO role2
    case CopyRolePrivileges(source, to, from, grantDeny) => (context, parameterMapping, securityContext) =>
      // This operator expects CreateRole(to) and RequireRole(from) to run as source, so we do not check for those
      UpdatingSystemCommandExecutionPlan("CopyPrivileges", normalExecutionEngine,
        s"""MATCH (to:Role {name: $$to})
           |WHERE to.justCreated = true
           |MATCH (from:Role {name: $$from})-[:$grantDeny]->(a:Action)
           |MERGE (to)-[g:$grantDeny]->(a)
           |RETURN from.name, to.name, count(g)""".stripMargin,
        VirtualValues.map(Array("from", "to"), Array(Values.stringValue(from), Values.stringValue(to))),
        QueryHandler.handleError(e => new IllegalStateException(s"Failed to create role '$to' as copy of '$from': Failed to copy privileges.", e)),
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // DROP ROLE [IF EXISTS] foo
    case DropRole(source, roleName) => (context, parameterMapping, securityContext) =>
      UpdatingSystemCommandExecutionPlan("DropRole", normalExecutionEngine,
        """MATCH (role:Role {name: $name}) DETACH DELETE role
          |RETURN role""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        QueryHandler
          .handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to delete the specified role '$roleName': Role does not exist.")))
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new IllegalStateException(s"Failed to delete the specified role '$roleName': $followerError", error)
            case error => new IllegalStateException(s"Failed to delete the specified role '$roleName'.", error)
          },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // GRANT ROLE foo TO user
    case GrantRoleToUser(source, roleName, userName) => (context, parameterMapping, securityContext) =>
      UpdatingSystemCommandExecutionPlan("GrantRoleToUser", normalExecutionEngine,
        """MATCH (r:Role {name: $role})
          |OPTIONAL MATCH (u:User {name: $user})
          |WITH r, u
          |MERGE (u)-[a:HAS_ROLE]->(r)
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array("role","user"), Array(Values.stringValue(roleName), Values.stringValue(userName))),
        QueryHandler
          .handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to grant role '$roleName' to user '$userName': Role does not exist.")))
          .handleError {
            case e: InternalException if e.getMessage.contains("ignore rows where a relationship node is missing") =>
              new InvalidArgumentsException(s"Failed to grant role '$roleName' to user '$userName': User does not exist.", e)
            case e: HasStatus if e.status() == Status.Cluster.NotALeader =>
              new IllegalStateException(s"Failed to grant role '$roleName' to user '$userName': $followerError", e)
            case e => new IllegalStateException(s"Failed to grant role '$roleName' to user '$userName'.", e)
          },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // REVOKE ROLE foo FROM user
    case RevokeRoleFromUser(source, roleName, userName) => (context, parameterMapping, securityContext) =>
      UpdatingSystemCommandExecutionPlan("RevokeRoleFromUser", normalExecutionEngine,
        """MATCH (r:Role {name: $role})
          |OPTIONAL MATCH (u:User {name: $user})
          |WITH r, u
          |OPTIONAL MATCH (u)-[a:HAS_ROLE]->(r)
          |DELETE a
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array("role", "user"), Array(Values.stringValue(roleName), Values.stringValue(userName))),
        QueryHandler.handleError {
          case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
            new IllegalStateException(s"Failed to revoke role '$roleName' from user '$userName': $followerError", error)
          case error => new IllegalStateException(s"Failed to revoke role '$roleName' from user '$userName'.", error)
        },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // GRANT/DENY/REVOKE TRAVERSE ON GRAPH foo NODES A (*) TO role
    case GrantTraverse(source, database, qualifier, roleName) => (context, parameterMapping, securityContext) =>
      makeGrantOrDenyExecutionPlan(ResourcePrivilege.Action.TRAVERSE.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext)), GRANT, s"Failed to grant traversal privilege to role '$roleName'")

    case DenyTraverse(source, database, qualifier, roleName) => (context, parameterMapping, securityContext) =>
      makeGrantOrDenyExecutionPlan(ResourcePrivilege.Action.TRAVERSE.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext)), DENY, s"Failed to deny traversal privilege to role '$roleName'")

    case RevokeTraverse(source, database, qualifier, roleName, revokeType) => (context, parameterMapping, securityContext) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.TRAVERSE.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName, revokeType,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext)), s"Failed to revoke traversal privilege from role '$roleName'")

    // GRANT/DENY/REVOKE READ {prop} ON GRAPH foo NODES A (*) TO role
    case GrantRead(source, resource, database, qualifier, roleName) => (context, parameterMapping, securityContext) =>
      makeGrantOrDenyExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext)), GRANT, s"Failed to grant read privilege to role '$roleName'")

    case DenyRead(source, resource, database, qualifier, roleName) => (context, parameterMapping, securityContext) =>
      makeGrantOrDenyExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext)), DENY, s"Failed to deny read privilege to role '$roleName'")

    case RevokeRead(source, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping, securityContext) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName, revokeType,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext)), s"Failed to revoke read privilege from role '$roleName'")

    // GRANT/DENY/REVOKE WRITE {*} ON GRAPH foo NODES * (*) TO role
    case GrantWrite(source, resource, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeGrantOrDenyExecutionPlan(ResourcePrivilege.Action.WRITE.toString, resource, database, qualifier, roleName,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)), GRANT, s"Failed to grant write privilege to role '$roleName'")

    case DenyWrite(source, resource, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeGrantOrDenyExecutionPlan(ResourcePrivilege.Action.WRITE.toString, resource, database, qualifier, roleName,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)), DENY, s"Failed to deny write privilege to role '$roleName'")

    case RevokeWrite(source, resource, database, qualifier, roleName, revokeType) => (context, parameterMapping, currentUser) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.WRITE.toString, resource, database, qualifier, roleName, revokeType,
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)), s"Failed to revoke write privilege from role '$roleName'")

    // SHOW [ALL | USER user | ROLE role] PRIVILEGES
    case ShowPrivileges(scope) => (_, _, _) =>
      val privilegeMatch =
        """
          |MATCH (r)-[g]->(a:Action)-[:SCOPE]->(s:Segment),
          |    (a)-[:APPLIES_TO]->(res:Resource),
          |    (s)-[:FOR]->(d),
          |    (s)-[:QUALIFIED]->(q)
          |WHERE d:Database OR d:DatabaseAll
        """.stripMargin
      val segmentColumn =
        """
          |CASE q.type
          |  WHEN 'node' THEN 'NODE('+q.label+')'
          |  WHEN 'relationship' THEN 'RELATIONSHIP('+q.label+')'
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
          |RETURN type(g) AS grant, a.action AS action, $resourceColumn AS resource,
          |coalesce(d.name, '*') AS graph, segment, r.name AS role
        """.stripMargin
      val orderBy =
        s"""
          |ORDER BY role, graph, segment, resource, action
        """.stripMargin

      val (grantee: Value, query) = scope match {
        case ast.ShowRolePrivileges(name) => (Values.stringValue(name),
          s"""
             |OPTIONAL MATCH (r:Role) WHERE r.name = $$grantee WITH r
             |$privilegeMatch
             |WITH g, a, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
             |$returnColumns
             |$orderBy
          """.stripMargin
        )
        case ast.ShowUserPrivileges(name) => (Values.stringValue(name),
          s"""
             |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = $$grantee WITH r, u
             |$privilegeMatch
             |WITH g, a, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, u.name, r.name, segment
             |$returnColumns, u.name AS user
             |$orderBy
          """.stripMargin
        )
        case ast.ShowAllPrivileges() => (Values.NO_VALUE,
          s"""
             |OPTIONAL MATCH (r:Role) WITH r
             |$privilegeMatch
             |WITH g, a, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
             |$returnColumns
             |$orderBy
          """.stripMargin
        )
        case _ => throw new IllegalStateException(s"Invalid show privilege scope '$scope'")
      }
      SystemCommandExecutionPlan("ShowPrivileges", normalExecutionEngine, query, VirtualValues.map(Array("grantee"), Array(grantee)))

    // SHOW DATABASES
    case ShowDatabases() => (_, _, _) =>
      SystemCommandExecutionPlan("ShowDatabases", normalExecutionEngine,
        "MATCH (d:Database) RETURN d.name as name, d.status as status, d.default as default", VirtualValues.EMPTY_MAP)

    // SHOW DEFAULT DATABASE
    case ShowDefaultDatabase() => (_, _, _) =>
      SystemCommandExecutionPlan("ShowDefaultDatabase", normalExecutionEngine,
        "MATCH (d:Database {default: true}) RETURN d.name as name, d.status as status", VirtualValues.EMPTY_MAP)

    // SHOW DATABASE foo
    case ShowDatabase(normalizedName) => (_, _, _) =>
      SystemCommandExecutionPlan("ShowDatabase", normalExecutionEngine,
        "MATCH (d:Database {name: $name}) RETURN d.name as name, d.status as status, d.default as default",
        VirtualValues.map(Array("name"), Array(Values.stringValue(normalizedName.name))))

    // CREATE [OR REPLACE] DATABASE [IF NOT EXISTS] foo
    case CreateDatabase(normalizedName, replace, allowExistingDatabase) => (_, _, _) =>
      // Ensuring we don't exceed the max number of databases is a separate step
      val dbName = normalizedName.name
      val defaultDbName = resolver.resolveDependency(classOf[Config]).get(GraphDatabaseSettings.default_database)
      val default = dbName.equals(defaultDbName)

      val clusterOptions = Try(resolver.resolveDependency(classOf[RaftMachine])) match {
        case Success(raftMachine) =>
          val initialMembersSet = raftMachine.coreState.committed.members.asScala
          val opts = (initialMembersSet.map(_.getUuid.toString).toArray, System.currentTimeMillis(),
            ThreadLocalRandom.current().nextLong(), Standard.LATEST_STORE_VERSION)
          Some(opts)

        case Failure(_) => None
      }

      val virtualKeys: Array[String] = Array("name", "status", "default")
      val virtualValues: Array[AnyValue] = Array(Values.stringValue(dbName), DatabaseStatus.Online, Values.booleanValue(default))

      val clusterProperties =
        """
          |  , // needed since it might be empty string instead
          |  d.initial_members = $initialMembers,
          |  d.store_creation_time = $creationTime,
          |  d.store_random_id = $randomId,
          |  d.store_version = $storeVersion """.stripMargin

      val (queryAdditions, virtualMap) = clusterOptions match {
        case Some(opts) =>
          val (initial, creation, randomId, storeVersion) = opts

          (clusterProperties, VirtualValues.map(
            virtualKeys ++ Array("initialMembers", "creationTime", "randomId", "storeVersion"),
            virtualValues ++ Array(Values.stringArray(initial:_*), Values.longValue(creation), Values.longValue(randomId), Values.stringValue(storeVersion))))

        case None => ("",VirtualValues.map(virtualKeys, virtualValues))
      }

      val query = if (replace) {
        s"""OPTIONAL MATCH (old:Database {name: $$name})
          |REMOVE old:Database
          |SET old:DeletedDatabase
          |SET old.deleted_at = datetime()
          |
          |CREATE (d:Database {name: $$name})
          |SET
          |  d.status = $$status,
          |  d.default = $$default,
          |  d.created_at = datetime(),
          |  d.uuid = randomUUID()
          |  $queryAdditions
          |RETURN d.name as name, d.status as status, d.uuid as uuid
        """.stripMargin
      } else if (allowExistingDatabase) {
        s"""MERGE (d:Database {name: $$name})
          |ON CREATE SET
          |  d.status = $$status,
          |  d.default = $$default,
          |  d.created_at = datetime(),
          |  d.uuid = randomUUID()
          |  $queryAdditions
          |RETURN d.name as name, d.status as status, d.uuid as uuid
        """.stripMargin
      } else {
        s"""CREATE (d:Database {name: $$name})
          |SET
          |  d.status = $$status,
          |  d.default = $$default,
          |  d.created_at = datetime(),
          |  d.uuid = randomUUID()
          |  $queryAdditions
          |RETURN d.name as name, d.status as status, d.uuid as uuid
        """.stripMargin
      }
      UpdatingSystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine, query,
        virtualMap, QueryHandler
          .handleError(error => (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new DatabaseExistsException(s"Failed to create the specified database '$dbName': Database already exists.", error)
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new IllegalStateException(s"Failed to create the specified database '$dbName': $followerError", error)
            case _ => new IllegalStateException(s"Failed to create the specified database '$dbName'.", error)
          })
      )

    // Used to ensure we don't create to many databases,
    // this by first creating/replacing (source) and then check we didn't exceed the allowed number
    case EnsureValidNumberOfDatabases(source) =>  (context, parameterMapping, securityContext) =>
      val dbName = source.get.normalizedName.name // database to be created, needed for error message
      val query =
        """MATCH (d:Database)
          |RETURN count(d) as numberOfDatabases
        """.stripMargin
      UpdatingSystemCommandExecutionPlan("EnsureValidNumberOfDatabases", normalExecutionEngine, query, VirtualValues.EMPTY_MAP,
        QueryHandler.handleResult((_, numberOfDatabases) =>
          if (numberOfDatabases.asInstanceOf[LongValue].longValue() > maxDBLimit) {
            Some(new DatabaseLimitReachedException(s"Failed to create the specified database '$dbName': "))
          } else {
            None
          }
        ),
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // DROP DATABASE [IF EXISTS] foo
    case DropDatabase(source, normalizedName) => (context, parameterMapping, securityContext) =>
      val dbName = normalizedName.name
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        """MATCH (d:Database {name: $name})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(dbName))),
        QueryHandler.handleError {
          case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
            new IllegalStateException(s"Failed to delete the specified database '$dbName': $followerError", error)
          case error => new IllegalStateException(s"Failed to delete the specified database '$dbName'.", error)
        },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // START DATABASE foo
    case StartDatabase(normalizedName) => (_, _, _) =>
      val dbName = normalizedName.name
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
        QueryHandler
          .handleResult((offset, value) => {
            if (offset == 2 && (value eq Values.NO_VALUE)) Some(new DatabaseNotFoundException(s"Failed to start the specified database '$dbName': Database does not exist."))
            else None
          })
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new IllegalStateException(s"Failed to start the specified database '$dbName': $followerError", error)
            case error => new IllegalStateException(s"Failed to start the specified database '$dbName'.", error)
          }
      )

    // STOP DATABASE foo
    case StopDatabase(source, normalizedName) => (context, parameterMapping, securityContext) =>
      val dbName = normalizedName.name
      UpdatingSystemCommandExecutionPlan("StopDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d2:Database {name: $name, status: $oldStatus})
          |SET d2.status = $status
          |SET d2.stopped_at = datetime()
          |RETURN d2.name as name, d2.status as status""".stripMargin,
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.stringValue(dbName),
            DatabaseStatus.Online,
            DatabaseStatus.Offline
          )
        ),
        QueryHandler.handleError {
          case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
            new IllegalStateException(s"Failed to stop the specified database '$dbName': $followerError", error)
          case error => new IllegalStateException(s"Failed to stop the specified database '$dbName'.", error)
        },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // Used to check whether a database is present and not the system database,
    // which means it can be dropped and stopped.
    case EnsureValidNonSystemDatabase(source, normalizedName, action) => (context, parameterMapping, securityContext) =>
      val dbName = normalizedName.name
      if (dbName.equals(SYSTEM_DATABASE_NAME))
        throw new DatabaseAdministrationException(s"Not allowed to $action system database.")

      UpdatingSystemCommandExecutionPlan("EnsureValidNonSystemDatabase", normalExecutionEngine,
        """MATCH (db:Database {name: $name})
          |RETURN db.name AS name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(dbName))),
        QueryHandler.handleNoResult(() =>
          Some(new DatabaseNotFoundException(s"Failed to $action the specified database '$dbName': Database does not exist."))),
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // Used to log commands
    case LogSystemCommand(source, command) => (context, parameterMapping, securityContext) =>
      LoggingSystemCommandExecutionPlan(
        fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping, securityContext),
        command,
        securityContext,
        (message, securityContext) => authManager.log(message, securityContext)
      )
  }

  private def makeGrantOrDenyExecutionPlan(actionName: String,
                                           resource: ast.ActionResource,
                                           database: ast.GraphScope,
                                           qualifier: ast.PrivilegeQualifier,
                                           roleName: String, source: Option[ExecutionPlan],
                                           grant: GrantOrDeny,
                                           startOfErrorMessage: String): UpdatingSystemCommandExecutionPlan = {

    val commandName = if (grant.isGrant) "GrantPrivilege" else "DenyPrivilege"

    val action = Values.stringValue(actionName)
    val role = Values.stringValue(roleName)
    val (property: Value, resourceType: Value, resourceMerge: String) = resource match {
      case ast.PropertyResource(name) => (Values.stringValue(name), Values.stringValue(Resource.Type.PROPERTY.toString), "MERGE (res:Resource {type: $resource, arg1: $property})")
      case ast.NoResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.GRAPH.toString), "MERGE (res:Resource {type: $resource})")
      case ast.AllResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.ALL_PROPERTIES.toString), "MERGE (res:Resource {type: $resource})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege ${grant.name} resource type $resource")
    }
    val (label: Value, qualifierMerge: String) = qualifier match {
      case ast.LabelQualifier(name) => (Values.stringValue(name), "MERGE (q:LabelQualifier {type: 'node', label: $label})")
      case ast.LabelAllQualifier() => (Values.NO_VALUE, "MERGE (q:LabelQualifierAll {type: 'node', label: '*'})") // The label is just for later printout of results
      case ast.RelationshipQualifier(name) => (Values.stringValue(name), "MERGE (q:RelationshipQualifier {type: 'relationship', label: $label})")
      case ast.RelationshipAllQualifier() => (Values.NO_VALUE, "MERGE (q:RelationshipQualifierAll {type: 'relationship', label: '*'})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege ${grant.name} qualifier $qualifier")
    }
    val (dbName, db, databaseMerge, scopeMerge) = database match {
      case ast.NamedGraphScope(name) => (Values.stringValue(name), name, "MATCH (d:Database {name: $database})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case ast.AllGraphsScope() => (Values.NO_VALUE, "*", "MERGE (d:DatabaseAll {name: '*'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege ${grant.name} scope database $database")
    }
    UpdatingSystemCommandExecutionPlan(commandName, normalExecutionEngine,
      s"""
         |// Find or create the segment scope qualifier (eg. label qualifier, or all labels)
         |$qualifierMerge
         |WITH q
         |
         |// Find the specified database, or find/create the special DatabaseAll node for '*'
         |$databaseMerge
         |WITH q, d
         |
         |// Create a new scope connecting the database to the qualifier using a :Segment node
         |$scopeMerge
         |
         |// Find or create the appropriate resource type (eg. 'graph') and then connect it to the scope through an :Action
         |$resourceMerge
         |MERGE (res)<-[:APPLIES_TO]-(a:Action {action: $$action})-[:SCOPE]->(s)
         |WITH q, d, a
         |
         |// Connect the role to the action to complete the privilege assignment
         |OPTIONAL MATCH (r:Role {name: $$role})
         |MERGE (r)-[:${grant.relType}]->(a)
         |
         |// Return the table of results
         |RETURN '${grant.prefix}' AS grant, a.action AS action, d.name AS database, q.label AS label, r.name AS role""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler
        .handleNoResult(() => Some(new DatabaseNotFoundException(s"$startOfErrorMessage: Database '$db' does not exist.")))
        .handleError {
          case e: InternalException if e.getMessage.contains("ignore rows where a relationship node is missing") =>
            new InvalidArgumentsException(s"$startOfErrorMessage: Role '$roleName' does not exist.", e)
          case e: HasStatus if e.status() == Status.Cluster.NotALeader => new IllegalStateException(s"$startOfErrorMessage: $followerError", e)
          case e => new IllegalStateException(s"$startOfErrorMessage.", e)
        },
      source
    )
  }

  private def makeRevokeExecutionPlan(actionName: String, resource: ast.ActionResource, database: ast.GraphScope, qualifier: ast.PrivilegeQualifier,
                                      roleName: String, revokeType: ast.RevokeType, source: Option[ExecutionPlan], startOfErrorMessage: String) = {
    val action = Values.stringValue(actionName)
    val role = Values.stringValue(roleName)
    val relType = if (revokeType.relType.nonEmpty) ":" + revokeType.relType else ""

    val (property: Value, resourceType: Value, resourceMatch: String) = resource match {
      case ast.PropertyResource(name) => (Values.stringValue(name), Values.stringValue(Resource.Type.PROPERTY.toString), "MATCH (res:Resource {type: $resource, arg1: $property})")
      case ast.NoResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.GRAPH.toString), "MATCH (res:Resource {type: $resource})")
      case ast.AllResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.ALL_PROPERTIES.toString), "MATCH (res:Resource {type: $resource})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege revoke resource type $resource")
    }
    val (label: Value, qualifierMatch: String) = qualifier match {
      case ast.LabelQualifier(name) => (Values.stringValue(name), "MATCH (q:LabelQualifier {type: 'node', label: $label})")
      case ast.LabelAllQualifier() => (Values.NO_VALUE, "MATCH (q:LabelQualifierAll {type: 'node', label: '*'})") // The label is just for later printout of results
      case ast.RelationshipQualifier(name) => (Values.stringValue(name), "MATCH (q:RelationshipQualifier {type: 'relationship', label: $label})")
      case ast.RelationshipAllQualifier() => (Values.NO_VALUE, "MATCH (q:RelationshipQualifierAll {type: 'relationship', label: '*'})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"$startOfErrorMessage: Invalid privilege revoke qualifier $qualifier")
    }
    val (dbName, _, scopeMatch) = database match {
      case ast.NamedGraphScope(name) => (Values.stringValue(name), name, "MATCH (d:Database {name: $database})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case ast.AllGraphsScope() => (Values.NO_VALUE, "*", "MATCH (d:DatabaseAll {name: '*'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
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
         |MATCH (res)<-[:APPLIES_TO]-(a:Action {action: $$action})-[:SCOPE]->(s)
         |
         |// Find the privilege assignment connecting the role to the action
         |OPTIONAL MATCH (r:Role {name: $$role})
         |WITH a, r, d, q
         |OPTIONAL MATCH (r)-[g$relType]->(a)
         |
         |// Remove the assignment
         |DELETE g
         |RETURN r.name AS role, g AS grant""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler.handleError {
        case e: HasStatus if e.status() == Status.Cluster.NotALeader => new IllegalStateException(s"$startOfErrorMessage: $followerError", e)
        case e => new IllegalStateException(s"$startOfErrorMessage.", e)
      },
      source
    )
  }

  override def isApplicableAdministrationCommand(logicalPlanState: LogicalPlanState): Boolean =
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable).isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
