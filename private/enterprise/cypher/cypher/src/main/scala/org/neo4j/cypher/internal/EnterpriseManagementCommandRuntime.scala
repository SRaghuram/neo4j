/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.util

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings
import com.neo4j.server.security.enterprise.auth._
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.DatabaseManagementException
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs._
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.dbms.api.{DatabaseExistsException, DatabaseLimitReachedException, DatabaseNotFoundException}
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.string.UTF8
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues

/**
  * This runtime takes on queries that require no planning, such as multidatabase management commands
  */
case class EnterpriseManagementCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver) extends ManagementCommandRuntime {
  val communityCommandRuntime: CommunityManagementCommandRuntime = CommunityManagementCommandRuntime(normalExecutionEngine, resolver)
  val maxDBLimit: Long = resolver.resolveDependency( classOf[Config] ).get(CommercialEditionSettings.maxNumberOfDatabases)

  override def name: String = "enterprise management-commands"

  private def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a recognized database administration command: ${unknownPlan.getClass.getSimpleName}")
  }

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext, username: String): ExecutionPlan = {

    val (planWithSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we delegate to communitys version of it (which supports common things like procedures)
    // If neither we throw an error
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable).applyOrElse(planWithSlottedParameters, throwCantCompile).apply(context, parameterMapping, username)
  }

  private lazy val authManager = {
    resolver.resolveDependency(classOf[CommercialAuthManager])
  }

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping, String) => ExecutionPlan] = {
    // SHOW USERS
    case ShowUsers() => (_, _, _) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |RETURN u.name as user, collect(r.name) as roles, u.passwordChangeRequired AS passwordChangeRequired, u.suspended AS suspended""".stripMargin,
        VirtualValues.EMPTY_MAP
      )

    // CREATE USER foo WITH PASSWORD password
    case CreateUser(userName, Some(initialStringPassword), None, requirePasswordChange, suspendedOptional) => (_, _, _) =>
      // TODO: Move the conversion to byte[] earlier in the stack (during or after parsing)
      val initialPassword = UTF8.encode(initialStringPassword)
      val suspended = suspendedOptional.getOrElse(false)
      try {
        validatePassword(initialPassword)

        // NOTE: If username already exists we will violate a constraint
        UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
          """CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})
            |RETURN u.name""".stripMargin,
          VirtualValues.map(
            Array("name", "credentials", "passwordChangeRequired", "suspended"),
            Array(
              Values.stringValue(userName),
              Values.stringValue(authManager.createCredentialForPassword(initialPassword).serialize()),
              Values.booleanValue(requirePasswordChange),
              Values.booleanValue(suspended))),
          QueryHandler
            .handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to create user '$userName'.")))
            .handleError(e => new InvalidArgumentsException(s"The specified user '$userName' already exists.", e))
            .handleResult((_, _) => clearCacheForUser(userName))
        )
      } finally {
        // Clear password
        if (initialPassword != null) util.Arrays.fill(initialPassword, 0.toByte)
      }

    // DROP USER foo
    case DropUser(userName) => (_, _, currentUser) =>
      if (userName.equals(currentUser)) throw new InvalidArgumentsException(s"Deleting yourself (user '$userName') is not allowed.")
      UpdatingSystemCommandExecutionPlan("DropUser", normalExecutionEngine,
        """MATCH (user:User {name: $name}) DETACH DELETE user
          |RETURN user""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(userName))),
        QueryHandler
          .handleNoResult(() => Some(new InvalidArgumentsException(s"User '$userName' does not exist.")))
          .handleError(e => new InvalidArgumentsException(s"Failed to delete the specified user '$userName'.", e))
          .handleResult((_, _) => clearCacheForUser(userName))
      )

    // ALTER USER foo
    case AlterUser(userName, initialStringPassword, None, requirePasswordChange, suspended) => (_, _, _) =>
      val params = Seq(
        initialStringPassword -> "credentials",
        requirePasswordChange -> "passwordChangeRequired",
        suspended -> "suspended"
      ).flatMap { param =>
        param._1 match {
          case None => Seq.empty
          case Some(value: Boolean) => Seq((param._2, Values.booleanValue(value)))
          case Some(value: String) =>
            Seq((param._2, Values.stringValue(authManager.createCredentialForPassword(validatePassword(UTF8.encode(value))).serialize())))
          case Some(p) => throw new InvalidArgumentsException(s"Invalid option type for ALTER USER, expected string or boolean but got: ${p.getClass.getSimpleName}")
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
          .handleNoResult(() => Some(new InvalidArgumentsException(s"User '$userName' does not exist.")))
          .handleError(e => new InvalidArgumentsException(s"Failed to alter the specified user '$userName'.", e))
          .handleResult((_, value) => {
            val maybeThrowable = initialStringPassword match {
              case Some(password) =>
                val oldCredentials = authManager.deserialize(value.asInstanceOf[TextValue].stringValue())
                if (oldCredentials.matchesPassword(UTF8.encode(password)))
                  Some(new InvalidArgumentsException("Old password and new password cannot be the same."))
                else
                  None
              case None => None
            }
            clearCacheForUser(userName)
            maybeThrowable
          })
      )

    // ALTER USER foo
    case AlterUser(_, _, Some(_), _, _) =>
      throw new IllegalStateException("Did not resolve parameters correctly.")

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

    // CREATE ROLE foo AS COPY OF bar
    case CreateRole(source, roleName) => (context, parameterMapping, currentUser) =>
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        """CREATE (new:Role {name: $new})
          |RETURN new.name""".stripMargin,
        VirtualValues.map(Array("new"), Array(Values.stringValue(roleName))),
        QueryHandler
          .handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to create '$roleName'.")))
          .handleError(e => new InvalidArgumentsException(s"The specified role '$roleName' already exists.", e)),
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser))
      )

    // Used to split the requirement from the source role before copying privileges
    case RequireRole(source, roleName) => (context, parameterMapping, currentUser) =>
      UpdatingSystemCommandExecutionPlan("RequireRole", normalExecutionEngine,
        """MATCH (role:Role {name: $name})
          |RETURN role.name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        QueryHandler.handleNoResult(() => Some(new InvalidArgumentsException(s"Role '$roleName' does not exist."))),
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser))
      )

    // COPY PRIVILEGES FROM role1 TO role2
    case CopyRolePrivileges(source, to, from, grantDeny) => (context, parameterMapping, currentUser) =>
      // This operator expects CreateRole(to) and RequireRole(from) to run as source, so we do not check for those
      UpdatingSystemCommandExecutionPlan("CopyPrivileges", normalExecutionEngine,
        s"""MATCH (to:Role {name: $$to})
           |MATCH (from:Role {name: $$from})-[:$grantDeny]->(a:Action)
           |MERGE (to)-[g:$grantDeny]->(a)
           |RETURN from.name, to.name, count(g)""".stripMargin,
        VirtualValues.map(Array("from", "to"), Array(Values.stringValue(from), Values.stringValue(to))),
        QueryHandler.handleError(e => new InvalidArgumentsException(s"Failed to copy privileges from '$from' to '$to'.", e)),
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser))
      )

    // DROP ROLE foo
    case DropRole(roleName) => (_, _, _) =>
      UpdatingSystemCommandExecutionPlan("DropRole", normalExecutionEngine,
        """MATCH (role:Role {name: $name}) DETACH DELETE role
          |RETURN role""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        QueryHandler
            .handleResult((_, _) => {
              clearCacheForAllUsers()
              clearCacheForRole(roleName)
            })
          .handleNoResult(() => Some(new InvalidArgumentsException(s"Role '$roleName' does not exist.")))
          .handleError(e => new InvalidArgumentsException(s"Failed to delete the specified role '$roleName'.", e))
      )

    // GRANT ROLE foo TO user
    case GrantRoleToUser(source, roleName, userName) => (context, parameterMapping, currentUser) =>
      UpdatingSystemCommandExecutionPlan("GrantRoleToUser", normalExecutionEngine,
        """MATCH (r:Role {name: $role})
          |OPTIONAL MATCH (u:User {name: $user})
          |WITH r, u
          |MERGE (u)-[a:HAS_ROLE]->(r)
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array("role","user"), Array(Values.stringValue(roleName), Values.stringValue(userName))),
        QueryHandler
          .handleResult((_,value) => clearCacheForUser(value.asInstanceOf[TextValue].stringValue()))
          .handleNoResult(() =>  Some(new InvalidArgumentsException(s"Cannot grant non-existent role '$roleName' to user '$userName'")))
          .handleError(e => new InvalidArgumentsException(s"Cannot grant role '$roleName' to non-existent user '$userName'", e)),
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser))
      )

    // REVOKE ROLE foo FROM user
    case RevokeRoleFromUser(source, roleName, userName) => (context, parameterMapping, currentUser) =>
      UpdatingSystemCommandExecutionPlan("RevokeRoleFromUser", normalExecutionEngine,
        """MATCH (r:Role {name: $role})
          |OPTIONAL MATCH (u:User {name: $user})
          |WITH r, u
          |OPTIONAL MATCH (u)-[a:HAS_ROLE]->(r)
          |DELETE a
          |RETURN u.name AS user""".stripMargin,
        VirtualValues.map(Array("role","user"), Array(Values.stringValue(roleName), Values.stringValue(userName))),
        QueryHandler
          .handleResult((_, u) => {
            if (!(u eq Values.NO_VALUE)) clearCacheForUser(u.asInstanceOf[TextValue].stringValue())
            else Some(new InvalidArgumentsException(s"Cannot revoke role '$roleName' from non-existent user '$userName'"))
          })
          .handleNoResult(() => Some(new InvalidArgumentsException(s"Cannot revoke non-existent role '$roleName' from user '$userName'"))),
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser))
      )

    // GRANT TRAVERSE ON GRAPH foo NODES A (*) TO role
    case GrantTraverse(source, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeGrantExecutionPlan(ResourcePrivilege.Action.FIND.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName,
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)))

    case RevokeTraverse(source, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.FIND.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName,
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)))

    // GRANT READ (prop) ON GRAPH foo NODES A (*) TO role
    case GrantRead(source, resource, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeGrantExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName,
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)))

    case RevokeRead(source, resource, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName,
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)))

    // GRANT WRITE (*) ON GRAPH foo NODES * (*) TO role
    case GrantWrite(source, resource, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeGrantExecutionPlan(ResourcePrivilege.Action.WRITE.toString, resource, database, qualifier, roleName,
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)))

    case RevokeWrite(source, resource, database, qualifier, roleName) => (context, parameterMapping, currentUser) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.WRITE.toString, resource, database, qualifier, roleName,
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser)))

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

      val (grantee: Value, query) = scope match {
        case ast.ShowRolePrivileges(name) => (Values.stringValue(name),
          s"""
             |OPTIONAL MATCH (r:Role) WHERE r.name = $$grantee WITH r
             |$privilegeMatch
             |WITH g, a, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
             |$returnColumns
          """.stripMargin
        )
        case ast.ShowUserPrivileges(name) => (Values.stringValue(name),
          s"""
             |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = $$grantee WITH r, u
             |$privilegeMatch
             |WITH g, a, res, d, $segmentColumn AS segment, r, u ORDER BY d.name, u.name, r.name, segment
             |$returnColumns, u.name AS user
          """.stripMargin
        )
        case ast.ShowAllPrivileges() => (Values.NO_VALUE,
          s"""
             |OPTIONAL MATCH (r:Role) WITH r
             |$privilegeMatch
             |WITH g, a, res, d, $segmentColumn AS segment, r ORDER BY d.name, r.name, segment
             |$returnColumns
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

    // CREATE DATABASE foo
    case CreateDatabase(normalizedName) => (_, _, _) =>
      val dbName = normalizedName.name

      val defaultDbName = resolver.resolveDependency(classOf[Config]).get(GraphDatabaseSettings.default_database)
      val default = dbName.equals(defaultDbName)

      UpdatingSystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        """MATCH (d:Database)
          |WITH count(d) as numberOfDatabases
          |
          |// the following basically acts as an "if" clause
          |FOREACH (ignoreMe in CASE WHEN numberOfDatabases < $maxNumberOfDatabases THEN [1] ELSE [] END |
          |  CREATE (d:Database {name: $name})
          |  SET d.status = $status
          |  SET d.default = $default
          |  SET d.created_at = datetime()
          |)
          |WITH "ignoreMe" as ignore
          |MATCH (d:Database {name: $name})
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name", "status", "default", "maxNumberOfDatabases"),
          Array(Values.stringValue(dbName), DatabaseStatus.Online, Values.booleanValue(default), Values.longValue(maxDBLimit))),
        QueryHandler
          .handleNoResult(() => Some(new DatabaseLimitReachedException()))
          .handleError(e => new DatabaseExistsException(s"The specified database '$dbName' already exists.", e))
      )

    // DROP DATABASE foo
    case DropDatabase(source, normalizedName) => (context, parameterMapping, currentUser) =>
      val dbName = normalizedName.name
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        """MATCH (d:Database {name: $name})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(dbName))),
        QueryHandler.handleResult((_, _) => clearCacheForAllRoles()),
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser))
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
        QueryHandler.handleResult((offset, value) => {
          if (offset == 2 && (value eq Values.NO_VALUE)) Some(new DatabaseNotFoundException("Database '" + dbName + "' does not exist."))
          else None
        })
      )

    // STOP DATABASE foo
    case StopDatabase(source, normalizedName) => (context, parameterMapping, currentUser) =>
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
        new QueryHandler,
        source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, currentUser))
      )

    // Used to check whether a database is present and not the system database,
    // which means it can be dropped and stopped.
    case EnsureValidNonDefaultDatabase(normalizedName, action) => (_, _, _) =>
      val dbName = normalizedName.name
      if (dbName.equals(SYSTEM_DATABASE_NAME))
        throw new DatabaseManagementException("Not allowed to " + action + " system database.")

      UpdatingSystemCommandExecutionPlan("EnsureValidNonDefaultDatabase", normalExecutionEngine,
        """MATCH (db:Database {name: $name})
          |RETURN db.name as name""".stripMargin,
        VirtualValues.map(
          Array("name"),
          Array(Values.stringValue(dbName))
        ),
        QueryHandler
          .handleNoResult(() => Some(new DatabaseNotFoundException("Database '" + dbName + "' does not exist.")))
      )
  }

  protected def clearCacheForUser(username: String): Option[Throwable] = {
    authManager.clearAuthCache()
    // Ultimately this should go to the trigger handling done in the reconciler
    None
  }

  protected def clearCacheForAllUsers(): Option[Throwable] = {
    authManager.clearAuthCache()
    // Ultimately this should go to the trigger handling done in the reconciler
    None
  }

  protected def clearCacheForRole(role: String): Option[Throwable]  = {
    authManager.clearCacheForRole(role)
    // Ultimately this should go to the trigger handling done in the reconciler
    None
  }

  protected def clearCacheForAllRoles(): Option[Throwable]  = {
    authManager.clearCacheForRoles()
    // Ultimately this should go to the trigger handling done in the reconciler
    None
  }

  private def makeGrantExecutionPlan(actionName: String, resource: ast.ActionResource, database: ast.GraphScope, qualifier: ast.PrivilegeQualifier, roleName: String, source: Option[ExecutionPlan]) = {
    val action = Values.stringValue(actionName)
    val role = Values.stringValue(roleName)
    val (property: Value, resourceType: Value, resourceMerge: String) = resource match {
      case ast.PropertyResource(name) => (Values.stringValue(name), Values.stringValue(Resource.Type.PROPERTY.toString), "MERGE (res:Resource {type: $resource, arg1: $property})")
      case ast.NoResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.GRAPH.toString), "MERGE (res:Resource {type: $resource})")
      case ast.AllResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.ALL_PROPERTIES.toString), "MERGE (res:Resource {type: $resource})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant resource type $resource")
    }
    val (label: Value, qualifierMerge: String) = qualifier match {
      case ast.LabelQualifier(name) => (Values.stringValue(name), "MERGE (q:LabelQualifier {type: 'node', label: $label})")
      case ast.LabelAllQualifier() => (Values.NO_VALUE, "MERGE (q:LabelQualifierAll {type: 'node', label: '*'})") // The label is just for later printout of resultscase ast.LabelQualifier(name) => (Values.stringValue(name), "MERGE (q:LabelQualifier {label: $label})")
      case ast.RelationshipQualifier(name) => (Values.stringValue(name), "MERGE (q:RelationshipQualifier {type: 'relationship', label: $label})")
      case ast.RelationshipAllQualifier() => (Values.NO_VALUE, "MERGE (q:RelationshipQualifierAll {type: 'relationship', label: '*'})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant qualifier $qualifier")
    }
    val (dbName, db, databaseMerge, scopeMerge) = database match {
      case ast.NamedGraphScope(name) => (Values.stringValue(name), name, "MATCH (d:Database {name: $database})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case ast.AllGraphsScope() => (Values.NO_VALUE, "*", "MERGE (d:DatabaseAll {name: '*'})", "MERGE (d)<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant scope database $database")
    }
    UpdatingSystemCommandExecutionPlan("GrantPrivilege", normalExecutionEngine,
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
         |MERGE (r)-[:GRANTED]->(a)
         |
         |// Return the table of results
         |RETURN 'GRANT' AS grant, a.action AS action, d.name AS database, q.label AS label, r.name AS role""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler
        .handleResult((offset, _) => {
          if (offset == 0) clearCacheForRole(roleName)
          None
        })
        .handleNoResult(() => Some(new DatabaseNotFoundException("Database '" + db + "' does not exist.")))
        .handleError(t => new InvalidArgumentsException("Role '" + roleName + "' does not exist.", t)),
      source
    )
  }

  private def makeRevokeExecutionPlan(actionName: String, resource: ast.ActionResource, database: ast.GraphScope, qualifier: ast.PrivilegeQualifier, roleName: String,source: Option[ExecutionPlan]) = {
    val action = Values.stringValue(actionName)
    val role = Values.stringValue(roleName)
    val (property: Value, resourceType: Value, resourceMatch: String) = resource match {
      case ast.PropertyResource(name) => (Values.stringValue(name), Values.stringValue(Resource.Type.PROPERTY.toString), "MATCH (res:Resource {type: $resource, arg1: $property})")
      case ast.NoResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.GRAPH.toString), "MATCH (res:Resource {type: $resource})")
      case ast.AllResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.ALL_PROPERTIES.toString), "MATCH (res:Resource {type: $resource})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant resource type $resource")
    }
    val (label: Value, qualifierMatch: String) = qualifier match {
      case ast.LabelQualifier(name) => (Values.stringValue(name), "MATCH (q:LabelQualifier {type: 'node', label: $label})")
      case ast.LabelAllQualifier() => (Values.NO_VALUE, "MATCH (q:LabelQualifierAll {type: 'node', label: '*'})") // The label is just for later printout of results
      case ast.RelationshipQualifier(name) => (Values.stringValue(name), "MATCH (q:RelationshipQualifier {type: 'relationship', label: $label})")
      case ast.RelationshipAllQualifier() => (Values.NO_VALUE, "MATCH (q:RelationshipQualifierAll {type: 'relationship', label: '*'})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant qualifier $qualifier")
    }
    val (dbName, _, scopeMatch) = database match {
      case ast.NamedGraphScope(name) => (Values.stringValue(name), name, "MATCH (d:Database {name: $database})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case ast.AllGraphsScope() => (Values.NO_VALUE, "*", "MATCH (d:DatabaseAll {name: '*'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant scope database $database")
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
         |OPTIONAL MATCH (r)-[g:GRANTED]->(a)
         |
         |// Remove the assignment
         |DELETE g
         |RETURN r.name AS role, g AS grant""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler
        .handleResult((offset, value) => {
          if (offset == 0 && (value eq Values.NO_VALUE)) Some(new InvalidArgumentsException(s"The role '$roleName' does not exist."))
          else if (offset == 1 && (value eq Values.NO_VALUE))Some(new InvalidArgumentsException(s"The role '$roleName' does not have the specified privilege: ${describePrivilege(actionName, resource, database, qualifier)}."))
          else clearCacheForRole(roleName)
        })
        .handleNoResult(() => Some(new InvalidArgumentsException(s"The privilege '${describePrivilege(actionName, resource, database, qualifier)}' does not exist."))),
      source
    )
  }

  private def describePrivilege(actionName: String, resource: ast.ActionResource, database: ast.GraphScope, qualifier: ast.PrivilegeQualifier): String = {
    // TODO: Improve description - or unify with main prettifier
    val (res, db, segment) = Prettifier.extractScope(resource, database, qualifier)
    s"$actionName $res ON GRAPH $db $segment"
  }

  override def isApplicableManagementCommand(logicalPlanState: LogicalPlanState): Boolean =
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable).isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
