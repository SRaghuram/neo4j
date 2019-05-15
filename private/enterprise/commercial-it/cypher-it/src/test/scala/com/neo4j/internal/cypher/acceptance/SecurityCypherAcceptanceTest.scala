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
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.{BasicPasswordPolicy, CommunitySecurityModule, SecureHasher}
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.server.security.systemgraph.{BasicSystemGraphRealm, ContextSwitchingSystemGraphQueryExecutor}
import org.neo4j.string.UTF8

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

  test("should list all default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL ROLES")

    // THEN
    result.toSet should be(defaultRoles)
  }

  test("should list populated default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true)))
  }

  test("should create and list roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")

    // THEN
    result.toSet should be(defaultRoles ++ Set(foo))
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
    val result = execute("SHOW ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should list all default roles with users") {
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
    result.toSet should be(Set(
      Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"),
      foo ++ Map("member" -> "Bar"),
      foo ++ Map("member" -> "Baz")
    ))
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

      fail("Expected error \"The specified role 'foo' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("The specified role 'foo' already exists.")
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

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Cannot create role 'bar' from non-existent role 'foo'.")
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

      fail("Expected error \"The specified role 'bar' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("The specified role 'bar' already exists.")
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

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Cannot create role 'bar' from non-existent role 'foo'.")
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

  test("should fail on dropping default role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES").toSet should be(defaultRoles)

    try {
      // WHEN
      execute(s"DROP ROLE ${PredefinedRoles.READER}")

      fail("Expected error \"'%s' is a predefined role and can not be deleted or modified.\" but succeeded.".format(PredefinedRoles.READER))
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("'%s' is a predefined role and can not be deleted or modified.".format(PredefinedRoles.READER))
    }

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should fail on dropping non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try {
      // WHEN
      execute("DROP ROLE foo")

      fail("Expected error \"Role 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Role 'foo' does not exist.")
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
      Map("user" -> "Bar", "roles" -> Seq("dragon", "fairy")),
      Map("user" -> "Baz", "roles" -> Seq.empty)
    ))

    getAllUserNamesFromManager should equal(Set("neo4j", "Bar", "Baz").asJava)
  }

  test("should create user with password as string") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER bar SET PASSWORD 'password'")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "bar", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "bar").asJava)
    val user = systemGraphRealm.getUser("bar")
    user.credentials().matchesPassword(UTF8.encode("password")) should be(true)
    user.credentials().matchesPassword(UTF8.encode("PASSword")) should be(false)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should create user with mixed password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER bar SET PASSWORD 'p4s5W*rd'")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "bar", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "bar").asJava)
    val user = systemGraphRealm.getUser("bar")
    user.credentials().matchesPassword(UTF8.encode("p4s5W*rd")) should be(true)
    user.credentials().matchesPassword(UTF8.encode("p4s5w*rd")) should be(false)
    user.credentials().matchesPassword(UTF8.encode("PASSword")) should be(false)
    user.credentials().matchesPassword(UTF8.encode("password")) should be(false)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should create user with password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> "bar"))

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("bar")) should be(true)
    user.credentials().matchesPassword(UTF8.encode("Bar")) should be(false)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should fail to create user with numeric password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    try {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> 123))

      fail("Expected error \"Only string values are accepted as password, got: Integer\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterWrongTypeException => e.getMessage should be("Only string values are accepted as password, got: Integer")
    }
  }

  test("should fail to create user with password as missing parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    try {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED")

      fail("Expected error \"Expected parameter(s): password\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterNotFoundException => e.getMessage should be("Expected parameter(s): password")
    }
  }

  test("should fail to create user with password as null parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    try {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> null))

      fail("Expected error \"Expected parameter(s): password\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterNotFoundException => e.getMessage should be("Expected parameter(s): password")
    }
  }

  test("should create user with password change not required") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("password")) should be(true)
    user.passwordChangeRequired() should equal(false)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should create user with status active") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("password")) should be(true)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should create user with status suspended") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDED")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("password")) should be(true)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(true)
  }

  test("should create user with all parameters") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED SET STATUS SUSPENDED")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("password")) should be(true)
    user.passwordChangeRequired() should equal(false)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(true)
  }

  test("should fail on creating already existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin"))
    ))
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    // WHEN
    try {
      execute("CREATE USER neo4j SET PASSWORD 'password'")

      fail("Expected error \"The specified user 'neo4j' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("The specified user 'neo4j' already exists.")
    }

    // THEN
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin"))
    ))
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)
  }

  test("should drop user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("DROP USER foo")

    // THEN
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin"))
    ))
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)
  }

  test("should fail on dropping non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)

    try {
      // WHEN
      execute("DROP USER foo")

      fail("Expected error \"User 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("User 'foo' does not exist.")
    }

    // THEN
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin"))
    ))
    getAllUserNamesFromManager should equal(Set("neo4j").asJava)
  }

  test("should alter user password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz'")

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("baz")) should be(true)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should alter user password with mixed upper- and lowercase letters") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'bAz'")

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("bAz")) should be(true)
    user.credentials().matchesPassword(UTF8.encode("BaZ")) should be(false)
    user.credentials().matchesPassword(UTF8.encode("BAZ")) should be(false)
    user.credentials().matchesPassword(UTF8.encode("baz")) should be(false)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should alter user password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password", Map("password" -> "baz"))

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("baz")) should be(true)
    user.credentials().matchesPassword(UTF8.encode("BAZ")) should be(false)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should fail on altering user password as missing parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    try {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password")

      fail("Expected error \"Expected parameter(s): password\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterNotFoundException => e.getMessage should be("Expected parameter(s): password")
    }
  }

  test("should alter user password mode") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.passwordChangeRequired() should equal(false)
    user.credentials().matchesPassword(UTF8.encode("bar")) should be(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should alter user status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET STATUS SUSPENDED")

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(true)
    user.credentials().matchesPassword(UTF8.encode("bar")) should be(true)
    user.passwordChangeRequired() should equal(true)
  }

  test("should alter user password and mode") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' CHANGE NOT REQUIRED")

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("baz")) should be(true)
    user.passwordChangeRequired() should equal(false)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(false)
  }

  test("should alter user password and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' SET STATUS SUSPENDED")

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("baz")) should be(true)
    user.passwordChangeRequired() should equal(true)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(true)
  }

  test("should alter user password mode and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED")

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("bar")) should be(true)
    user.passwordChangeRequired() should equal(false)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(true)
  }

  test("should alter user on all points") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED", Map("password" -> "baz"))

    // THEN
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)
    val user = systemGraphRealm.getUser("foo")
    user.credentials().matchesPassword(UTF8.encode("baz")) should be(true)
    user.passwordChangeRequired() should equal(false)
    user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(true)
  }

  test("should fail on alter user password as list parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar'")
    execute("SHOW USERS").toSet should be(Set(
      Map("user" -> "neo4j", "roles" -> Seq("admin")),
      Map("user" -> "foo", "roles" -> Seq.empty)
    ))
    getAllUserNamesFromManager should equal(Set("neo4j", "foo").asJava)

    try {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> Seq("baz", "boo")))

      fail("Expected error \"Only string values are accepted as password, got: List\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterWrongTypeException => e.getMessage should be("Only string values are accepted as password, got: List")
    }
  }

  // The systemGraphInnerQueryExecutor is needed for test setup with multiple users
  // But it can't be initialized until after super.initTest()
  private var systemGraphInnerQueryExecutor: ContextSwitchingSystemGraphQueryExecutor = _

  protected override def initTest(): Unit = {
    super.initTest()

    systemGraphInnerQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager(), threadToStatementContextBridge())
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(systemGraphInnerQueryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer =
      new SystemGraphInitializer(systemGraphInnerQueryExecutor, systemGraphOperations, importOptions, secureHasher, mock[Log], Config.defaults())
    val transactionEventListeners = graph.getDependencyResolver.resolveDependency(classOf[GlobalTransactionEventListeners])
    val systemListeners = transactionEventListeners.getDatabaseTransactionEventListeners(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    systemListeners.forEach(l => transactionEventListeners.unregisterTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, l))
    systemGraphInitializer.initializeSystemGraph()
    systemListeners.forEach(l => transactionEventListeners.registerTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, l))

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

  private def threadToStatementContextBridge() = {
    dependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
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
