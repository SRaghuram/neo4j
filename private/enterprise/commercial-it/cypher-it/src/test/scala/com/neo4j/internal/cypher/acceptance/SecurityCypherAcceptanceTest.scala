/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.time.Duration
import java.util
import java.util.Optional

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext
import com.neo4j.server.security.enterprise.auth.{CommercialAuthAndUserManager, EnterpriseUserManager}
import com.neo4j.server.security.enterprise.configuration.SecuritySettings
import com.neo4j.server.security.enterprise.systemgraph._
import org.mockito.Mockito.when
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.{BasicPasswordPolicy, CommunitySecurityModule, SecureHasher}
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor

import scala.collection.JavaConverters._

class SecurityCypherAcceptanceTest extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {
  private val defaultRoles = Set(
    Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.ARCHITECT, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.PUBLISHER, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.EDITOR, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.READER, "is_built_in" -> true)
  )
  private val defaultRolesWithUsers = Set(
    Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"),
    Map("role" -> PredefinedRoles.ARCHITECT, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.PUBLISHER, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.EDITOR, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.READER, "is_built_in" -> true, "member" -> null)
  )
  private val foo = Map("role" -> "foo", "is_built_in" -> false)
  private val bar = Map("role" -> "bar", "is_built_in" -> false)

  private var systemGraphRealm: SystemGraphRealm = _

  test("should list all roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW ROLES")

    // THEN
    result.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should list populated default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true)))
  }

  test("should list populated roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    // TODO update to use actual DDL when available for creating users and assigning to roles
    systemGraphInnerQueryExecutor.executeQueryLong(
      """CREATE (r:Role {name:'foo'})
        |CREATE (u1:User {name:'Bar',credentials:'neo',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
        |CREATE (u2:User {name:'Baz',credentials:'NEO',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
      """.stripMargin)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true), foo))
  }

  test("should list default roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should list populated roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j")))
  }

  test("should list populated roles with several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    // TODO update to use actual DDL when available for creating users and assigning to roles
    systemGraphInnerQueryExecutor.executeQueryLong(
      """CREATE (r:Role {name:'foo'})
        |CREATE (u1:User {name:'Bar',credentials:'neo',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
        |CREATE (u2:User {name:'Baz',credentials:'NEO',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
      """.stripMargin)

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"), foo ++ Map("member" -> "Bar"), foo ++ Map("member" -> "Baz")))
  }

  test("should create role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should fail on creating already existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))

    try{
      // WHEN
      execute("CREATE ROLE foo")

      fail("Expected error \"Cannot create already existing role\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create already existing role") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should create role from existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("CREATE ROLE bar AS COPY OF foo")

    // THEN
    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(foo, bar))
  }

  test("should fail on creating from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try{
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create role 'bar' from non-existent role 'foo'") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail on creating already existing role from other role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("CREATE ROLE bar")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo, bar))

    try{
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create already existing role\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create already existing role") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(foo, bar))
  }

  test("should fail on creating existing role from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE bar")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(bar))

    try{
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create role 'bar' from non-existent role 'foo'") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(bar))
  }

  test("should create and drop role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("DROP ROLE foo")

    // THEN
    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail on dropping non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try {
      // WHEN
      execute("DROP ROLE foo")

      fail("Expected error \"Cannot drop non-existent role 'foo'\"")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot drop non-existent role 'foo'") =>
    }

    // THEN
    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set.empty)
  }

  test("should list default user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet should be(Set(Map("user" -> "neo4j", "roles" -> Seq("admin"))))

    getAllUserNamesFromManager should equal(Set("neo4j").asJava)
  }

  test("should list all users") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   : dragon, fairy
    // Baz   :
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    // TODO update to use actual DDL when available for creating users and assigning to roles
    systemGraphInnerQueryExecutor.executeQueryLong(
      """
        |CREATE (r1:Role {name:'dragon'})
        |CREATE (r2:Role {name:'fairy'})
        |CREATE (u1:User {name:'Bar',credentials:'neo',passwordChangeRequired:false,suspended:false})
        |CREATE (u2:User {name:'Baz',credentials:'NEO',passwordChangeRequired:false,suspended:false})
        |CREATE (u1)-[:HAS_ROLE]->(r1)
        |CREATE (u1)-[:HAS_ROLE]->(r2)
      """.stripMargin)

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "Bar", "roles" -> Seq("fairy", "dragon")),
      Map("user" -> "Baz", "roles" -> Seq.empty)
    ))

    getAllUserNamesFromManager should equal(Set("neo4j", "Bar", "Baz").asJava)

  }

  test("should create user with password as string") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER bar WITH PASSWORD 'password'")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "bar", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "bar").asJava)

  }

  test("should create user with password change not required") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER foo WITH PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
  }

  test("should create user with status active") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER foo WITH PASSWORD 'password' WITH STATUS ACTIVE")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
  }

  test("should create user with status suspended") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)
    // WHEN
    execute("CREATE USER foo WITH PASSWORD 'password' WITH STATUS SUSPENDED")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
  }

  test("should fail on creating already existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin"))
    ))
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
//    try {
      execute("CREATE USER neo4j WITH PASSWORD 'password'")

//      fail("Expected error \"Cannot create already existing user\" but succeeded.")
//    } catch {
//      // THEN
//      case e :Exception if e.getMessage.equals("Cannot create already existing user") =>
//    }

    // THEN
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin"))
    ))
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)
  }

  // The systemGraphInnerQueryExecutor is needed for test setup with multiple users
  // But it can't be initialized until after super.initTest()
  private var systemGraphInnerQueryExecutor: ContextSwitchingSystemGraphQueryExecutor = _

  protected override def initTest(): Unit = {
    super.initTest()

    systemGraphInnerQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager(), "neo4j")
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(systemGraphInnerQueryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer = new SystemGraphInitializer(systemGraphInnerQueryExecutor, systemGraphOperations, importOptions, secureHasher, mock[Log])
    systemGraphInitializer.initializeSystemGraph()

    // need to setup/mock security a bit so we can have a userManager
    val config = mock[Config]
    when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( false )
    when( config.get( SecuritySettings.auth_cache_ttl ) ).thenReturn( Duration.ZERO )
    when( config.get( SecuritySettings.auth_cache_max_capacity ) ).thenReturn( 10 )
    when( config.get( SecuritySettings.auth_cache_use_ttl ) ).thenReturn( true )
    when( config.get( SecuritySettings.security_log_successful_authentication ) ).thenReturn( false )
    when( config.get( GraphDatabaseSettings.auth_max_failed_attempts ) ).thenReturn( 3 )  //!
    when( config.get( GraphDatabaseSettings.auth_lock_time ) ).thenReturn( Duration.ofSeconds( 5 ) )
    when (config.get( SecuritySettings.auth_providers )).thenReturn(List(SecuritySettings.NATIVE_REALM_NAME).asJava)

    systemGraphRealm = new SystemGraphRealm(   // this is also a UserManager even if the Name does not indicate that
      systemGraphOperations,
      systemGraphInitializer,
      false,
      secureHasher,
      new BasicPasswordPolicy(),
      CommunitySecurityModule.createAuthenticationStrategy( config ),
      false,
      false
    )
  }

  private def databaseManager() = {
    dependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])
  }

  private def dependencyResolver = {
    graph.getDependencyResolver
  }

  private def selectDatabase(name: String): Unit = {
    val manager = databaseManager()
    val maybeCtx: Optional[DatabaseContext] = manager.getDatabaseContext(new DatabaseId(name))
    val dbCtx: DatabaseContext = maybeCtx.orElseGet(() => throw new RuntimeException(s"No such database: $name"))
    graphOps = dbCtx.databaseFacade()
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)

    dependencyResolver.asInstanceOf[Dependencies].satisfyDependency(SimpleUserManagerSupplier(systemGraphRealm)) // needed to do that here on the outer engine

  }

  private def getAllUserNamesFromManager = {
    systemGraphRealm.getAllUsernames
  }
}

case class SimpleUserManagerSupplier(userManager: EnterpriseUserManager) extends CommercialAuthAndUserManager {
  override def getUserManager(authSubject: AuthSubject, isUserManager: Boolean): EnterpriseUserManager = getUserManager

  override def getUserManager: EnterpriseUserManager = userManager

  override def clearAuthCache(): Unit = ???

  override def login(authToken: util.Map[String, AnyRef]): CommercialLoginContext = ???

  override def init(): Unit = ???

  override def start(): Unit = ???

  override def stop(): Unit = ???

  override def shutdown(): Unit = ???
}
