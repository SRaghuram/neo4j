/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.lang.String.format
import java.util
import java.util.regex.Pattern

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager
import com.neo4j.server.security.enterprise.auth._
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs._
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.dbms.api.{DatabaseExistsException, DatabaseNotFoundException}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.server.security.auth.SecureHasher
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.server.security.systemgraph.SystemGraphCredential
import org.neo4j.string.UTF8
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues

/**
  * This runtime takes on queries that require no planning, such as multidatabase management commands
  */
case class EnterpriseManagementCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver) extends ManagementCommandRuntime {
  val communityCommandRuntime: CommunityManagementCommandRuntime = CommunityManagementCommandRuntime(normalExecutionEngine)
  val secureHasher = new SecureHasher()

  override def name: String = "enterprise management-commands"

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext, username: String): ExecutionPlan = {

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
      .applyOrElse(withSlottedParameters, throwCantCompile).apply(context, parameterMapping, username)
  }

  private lazy val authManager = {
    resolver.resolveDependency(classOf[CommercialAuthManager])
  }

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, Map[String, Int], String) => ExecutionPlan] = {
    // SHOW USERS
    case ShowUsers() => (_, _, _) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |RETURN u.name as user, collect(r.name) as roles, u.passwordChangeRequired AS passwordChangeRequired, u.suspended AS suspended""".stripMargin,
        VirtualValues.EMPTY_MAP
      )

    // CREATE USER foo WITH PASSWORD password
    case CreateUser(userName, Some(initialStringPassword), None, requirePasswordChange, suspended) => (_, _, _) =>
      // TODO: Move the conversion to byte[] earlier in the stack (during or after parsing)
      val initialPassword = UTF8.encode(initialStringPassword)
      try {
        assertValidUsername(userName)
        validatePassword(initialPassword)

        // NOTE: If username already exists we will violate a constraint
        UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
          """CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})
            |RETURN u.name""".stripMargin,
          VirtualValues.map(
            Array("name", "credentials", "passwordChangeRequired", "suspended"),
            Array(
              Values.stringValue(userName),
              Values.stringValue(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize()),
              Values.booleanValue(requirePasswordChange),
              Values.booleanValue(suspended))),
          QueryHandler
            .handleNoResult(() => throw new InvalidArgumentsException(s"Failed to create user '$userName'."))
            .handleError(e => throw new InvalidArgumentsException(s"The specified user '$userName' already exists.", e))
            .handleResult(_ => clearCacheForUser(userName))
        )
      } finally {
        // Clear password
        if (initialPassword != null) util.Arrays.fill(initialPassword, 0.toByte)
      }

    // CREATE USER foo WITH PASSWORD $password
    case CreateUser(_, _, Some(_), _, _) =>
      throw new IllegalStateException("Did not resolve parameters correctly.")

    // CREATE USER foo WITH PASSWORD
    case CreateUser(_, _, _, _, _) =>
      throw new IllegalStateException("Password not correctly supplied.")

    // DROP USER foo
    case DropUser(userName) => (_, _, currentUser) =>
      if (userName.equals(currentUser)) throw new InvalidArgumentsException(s"Deleting yourself (user '$userName') is not allowed.")
      UpdatingSystemCommandExecutionPlan("DropUser", normalExecutionEngine,
        """MATCH (user:User {name: $name}) DETACH DELETE user
          |RETURN user""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(userName))),
        QueryHandler
          .handleNoResult(() => throw new InvalidArgumentsException(s"User '$userName' does not exist."))
          .handleError(e => throw new InvalidArgumentsException(s"Failed to delete the specified user '$userName'.", e))
          .handleResult(_ => clearCacheForUser(userName))
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
            Seq((param._2, Values.stringValue(SystemGraphCredential.createCredentialForPassword(validatePassword(UTF8.encode(value)), secureHasher).serialize())))
          case Some(p) => throw new IllegalArgumentException(s"Invalid option type for ALTER USER, expected string or boolean but got: ${p.getClass.getSimpleName}")
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
          .handleNoResult(() => throw new InvalidArgumentsException(s"User '$userName' does not exist."))
          .handleError(e => throw new InvalidArgumentsException(s"Failed to alter the specified user '$userName'.", e))
          .handleResult(row => {
            initialStringPassword.foreach(password => {
              val oldCredentials = SystemGraphCredential.deserialize(row.get("oldCredentials").asInstanceOf[String], secureHasher)
              if (oldCredentials.matchesPassword(UTF8.encode(password))) throw new InvalidArgumentsException("Old password and new password cannot be the same.")
            })
            clearCacheForUser(userName)
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
    case CreateRole(roleName, Some(from)) => (_, _, _) =>
      assertValidRoleName(roleName)
      val names: Array[AnyValue] = Array(Values.stringValue(from), Values.stringValue(roleName))
      UpdatingSystemCommandExecutionPlan("CopyRole", normalExecutionEngine,
        """MATCH (old:Role {name: $old})
          |CREATE (new:Role {name: $new})
          |RETURN old.name, new.name""".stripMargin,
        VirtualValues.map(Array("old", "new"), names),
        QueryHandler
          .handleNoResult(() => throw new InvalidArgumentsException(s"Cannot create role '$roleName' from non-existent role '$from'."))
          .handleError(e => throw new InvalidArgumentsException(s"The specified role '$roleName' already exists.", e))
      )

    // CREATE ROLE foo
    case CreateRole(roleName, _) => (_, _, _) =>
      assertValidRoleName(roleName)
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        """CREATE (role:Role {name: $name})
          |RETURN role""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        QueryHandler
          .handleError(e => throw new InvalidArgumentsException(s"The specified role '$roleName' already exists.", e))
      )

    // DROP ROLE foo
    case DropRole(roleName) => (_, _, _) =>
      assertNotPredefinedRoleName(roleName)
      UpdatingSystemCommandExecutionPlan("DropRole", normalExecutionEngine,
        """MATCH (role:Role {name: $name}) DETACH DELETE role
          |RETURN role""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        QueryHandler
          .handleNoResult(() => throw new InvalidArgumentsException(s"Role '$roleName' does not exist."))
          .handleError(e => throw new InvalidArgumentsException(s"Failed to delete the specified role '$roleName'.", e))
      )

    // GRANT ROLE foo TO user
    case GrantRolesToUsers(roleNames, userNames) => (_, _, _) =>
      val roles = Values.stringArray(roleNames: _*)
      val users = Values.stringArray(userNames: _*)
      UpdatingSystemCommandExecutionPlan("GrantRoleToUser", normalExecutionEngine,
        """UNWIND $roles AS role
          |UNWIND $users AS user
          |MATCH (r:Role {name: role}), (u:User {name: user})
          |MERGE (u)-[a:HAS_ROLE]->(r)
          |RETURN user, collect(role) AS roles""".stripMargin,
        VirtualValues.map(Array("roles", "users"), Array(roles, users)),
        QueryHandler.handleResult(row => clearCacheForUser(row.get("user").toString))
      )

    // REVOKE ROLE foo FROM user
    case RevokeRolesFromUsers(roleNames, userNames) => (_, _, _) =>
      val roles = Values.stringArray(roleNames: _*)
      val users = Values.stringArray(userNames: _*)
      UpdatingSystemCommandExecutionPlan("RevokeRoleFromUser", normalExecutionEngine,
        """UNWIND $roles AS role
          |UNWIND $users AS user
          |MATCH (u:User {name: user})-[a:HAS_ROLE]->(r:Role {name: role})
          |DELETE a
          |RETURN user, collect(role) AS roles""".stripMargin,
        VirtualValues.map(Array("roles", "users"), Array(roles, users)),
        QueryHandler.handleResult(row => clearCacheForUser(row.get("user").toString))
      )

    // GRANT TRAVERSE ON GRAPH foo NODES A (*) TO role
    case GrantTraverse(database, qualifier, roleName) => (_, _, _) =>
      makeGrantExecutionPlan(ResourcePrivilege.Action.FIND.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName)

    case RevokeTraverse(database, qualifier, roleName) => (_, _, _) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.FIND.toString, ast.NoResource()(InputPosition.NONE), database, qualifier, roleName)

    // GRANT READ (prop) ON GRAPH foo NODES A (*) TO role
    case GrantRead(resource, database, qualifier, roleName) => (_, _, _) =>
      makeGrantExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName)

    case RevokeRead(resource, database, qualifier, roleName) => (_, _, _) =>
      makeRevokeExecutionPlan(ResourcePrivilege.Action.READ.toString, resource, database, qualifier, roleName)

    // SHOW [ALL | USER user | ROLE role] PRIVILEGES
    case ShowPrivileges(scope) => (_, _, _) =>
      val privilegeMatch =
        """
          |MATCH (r)-[g]->(a:Action)-[:SCOPE]->(s:Segment),
          |    (a)-[:APPLIES_TO]->(res:Resource),
          |    (s)-[:FOR]->(d),
          |    (s)-[:QUALIFIED]->(q)
          |
        """.stripMargin
      val returnColumns =
        """
          |RETURN type(g) AS grant, a.action AS action, res.type AS resource, res.arg1 AS arg1,
          |coalesce(d.name, '*') AS database, q.label AS label, r.name AS role
        """.stripMargin

      val (grantee: Value, query) = scope match {
        case ast.ShowRolePrivileges(name) => (Values.stringValue(name),
          s"""
             |OPTIONAL MATCH (r:Role) WHERE r.name = $$grantee WITH r
             |$privilegeMatch
             |WITH g, a, res, d, q, r ORDER BY d.name, r.name, q.label
             |$returnColumns
          """.stripMargin
        )
        case ast.ShowUserPrivileges(name) => (Values.stringValue(name),
          s"""
             |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = $$grantee WITH r, u
             |$privilegeMatch
             |WITH g, a, res, d, q, r, u ORDER BY d.name, u.name, r.name, q.label
             |$returnColumns, u.name AS user
          """.stripMargin
        )
        case ast.ShowAllPrivileges() => (Values.NO_VALUE,
          s"""
             |OPTIONAL MATCH (r:Role) WITH r
             |$privilegeMatch
             |WITH g, a, res, d, q, r ORDER BY d.name, r.name, q.label
             |$returnColumns
          """.stripMargin
        )
        case _ => throw new IllegalStateException(s"Invalid show privilege scope '$scope'")
      }
      SystemCommandExecutionPlan("ShowPrivileges", normalExecutionEngine, query,
        VirtualValues.map(Array("grantee"), Array(grantee)),
        (q: QueryExecution) => new SystemCommandExecutionResult(q.asInstanceOf[InternalExecutionResult]) {
          override def fieldNames(): Array[String] = {
            val fields = inner.fieldNames()
            fields.slice(0, 3) ++ fields.slice(4, fields.length)
          }

          lazy val innerIterator: ResourceIterator[util.Map[String, AnyRef]] = inner.javaIterator

          override def asIterator: ResourceIterator[util.Map[String, AnyRef]] = new ResourceIterator[util.Map[String, AnyRef]] {
            override def close(): Unit = innerIterator.close()

            override def hasNext: Boolean = innerIterator.hasNext

            override def next(): util.Map[String, AnyRef] = {
              import scala.collection.JavaConverters._
              val row = innerIterator.next()
              val mapped = fieldNames().foldLeft(Map.empty[String, AnyRef]) { (a, k) =>
                if (k == "resourceArg1") a
                else if (k == "resource" && row.get(k) == "property") a + (k -> s"property(${row.get("resourceArg1")})")
                else a + (k -> row.get(k))
              }
              mapped.asJava
            }
          }

          override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
            inner.accept(new QueryResultVisitor[EX] {
              override def visit(row: QueryResult.Record): Boolean = {
                val mappedRow = new QueryResult.Record() {
                  override def fields(): Array[AnyValue] = {
                    val fields = row.fields()
                    if (fields(2).equals(Values.stringValue("property"))) {
                      val property = fields(3).asInstanceOf[StringValue].stringValue()
                      fields.slice(0, 2) ++ Array(Values.stringValue(s"property($property)")) ++ fields.slice(4, fields.length)
                    } else {
                      fields.slice(0, 3) ++ fields.slice(4, fields.length)
                    }
                  }
                }
                visitor.visit(mappedRow)
              }
            })
          }

        },
        e => throw new InvalidArgumentsException(s"The specified grantee '${grantee.asObject()}' does not exist.", e)
      )

    // CREATE DATABASE foo
    case CreateDatabase(dbName) => (_, _, _) =>
      SystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        """CREATE (d:Database {name: $name})
          |SET d.status = $status
          |Set d.default = false
          |SET d.created_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name", "status"), Array(Values.stringValue(dbName.toLowerCase), DatabaseStatus.Online)),
        onError = e => throw new DatabaseExistsException(s"The specified database '$dbName' already exists.", e)
      )

    // DROP DATABASE foo
    case DropDatabase(dbName) => (_, _, _) =>
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

  protected def clearCacheForUser(username: String): Unit = {
    // TODO: Get handle on BasicSystemGraphRealm to clear the cache
    //See: BasicSystemGraphRealm.clearCacheForUser(username)
    authManager.clearAuthCache()

    // Ultimately this should go to the trigger handling done in the reconciler
  }

  // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
  private val usernamePattern = Pattern.compile("^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$")

  def assertValidUsername(username: String): Unit = {
    if (username == null || username.isEmpty) throw new InvalidArgumentsException("The provided username is empty.")
    if (!usernamePattern.matcher(username).matches) throw new InvalidArgumentsException("Username '" + username + "' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces.")
  }

  private def validatePassword(password: Array[Byte]): Array[Byte] = {
    if (password == null || password.length == 0) throw new InvalidArgumentsException("A password cannot be empty.")
    password
  }

  private val roleNamePattern = Pattern.compile("^[a-zA-Z0-9_]+$")

  private def assertValidRoleName(name: String): Unit = {
    if (name == null || name.isEmpty) throw new InvalidArgumentsException("The provided role name is empty.")
    if (!roleNamePattern.matcher(name).matches) throw new InvalidArgumentsException("Role name '" + name + "' contains illegal characters. Use simple ascii characters and numbers.")
  }

  private def assertNotPredefinedRoleName(roleName: String): Unit = {
    // TODO: Find a way to not depend on enterprise security module
    if (roleName != null && PredefinedRolesBuilder.roles.keySet.contains(roleName)) throw new InvalidArgumentsException(format("'%s' is a predefined role and can not be deleted or modified.", roleName))
  }

  private def makeGrantExecutionPlan(actionName: String, resource: ast.ActionResource, database: ast.GraphScope, qualifier: ast.PrivilegeQualifier, roleName: String) = {
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
      QueryHandler.handleError(t => throw new InvalidArgumentsException("Role '" + roleName + "' does not exist.", t)
      ).handleNoResult(() => throw new DatabaseNotFoundException("Database '" + db + "' does not exist."))
    )
  }

  private def makeRevokeExecutionPlan(actionName: String, resource: ast.ActionResource, database: ast.GraphScope, qualifier: ast.PrivilegeQualifier, roleName: String) = {
    val action = Values.stringValue(actionName)
    val role = Values.stringValue(roleName)
    val (property: Value, resourceType: Value, resourceMatch: String) = resource match {
      case ast.PropertyResource(name) => (Values.stringValue(name), Values.stringValue(Resource.Type.PROPERTY.toString), "MATCH (res:Resource {type: $resource, arg1: $property})")
      case ast.NoResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.GRAPH.toString), "MATCH (res:Resource {type: $resource})")
      case ast.AllResource() => (Values.NO_VALUE, Values.stringValue(Resource.Type.ALL_PROPERTIES.toString), "MATCH (res:Resource {type: $resource})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant resource type $resource")
    }
    val (label: Value, qualifierMatch: String) = qualifier match {
      case ast.LabelQualifier(name) => (Values.stringValue(name), "MATCH (q:LabelQualifier {label: $label})")
      case ast.AllQualifier() => (Values.NO_VALUE, "MATCH (q:LabelQualifierAll {label: '*'})") // The label is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant qualifier $qualifier")
    }
    val (dbName, db, scopeMatch) = database match {
      case ast.NamedGraphScope(name) => (Values.stringValue(name), name, "MATCH (d:Database {name: $database})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)")
      case ast.AllGraphsScope() => (Values.NO_VALUE, "*", "MATCH (d:DatabaseAll {name: '*'})<-[:FOR]-(s:Segment)-[:QUALIFIED]->(q)") // The name is just for later printout of results
      case _ => throw new IllegalStateException(s"Invalid privilege grant scope database $database")
    }
    UpdatingSystemCommandExecutionPlan("GrantTraverse", normalExecutionEngine,
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
         |MATCH (r:Role {name: $$role})-[g:GRANTED]->(a)
         |
         |// Remove the assignment
         |DELETE g
         |RETURN g AS grant, a.action AS action, d.name AS database, q.label AS label, r.name AS role""".stripMargin,
      VirtualValues.map(Array("action", "resource", "property", "database", "label", "role"), Array(action, resourceType, property, dbName, label, role)),
      QueryHandler.handleNoResult(() => throw new InvalidArgumentsException("The privilege or the role '" + roleName + "' does not exist."))
    )
  }

  override def isApplicableManagementCommand(logicalPlanState: LogicalPlanState): Boolean =
    (logicalToExecutable orElse communityCommandRuntime.logicalToExecutable).isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
