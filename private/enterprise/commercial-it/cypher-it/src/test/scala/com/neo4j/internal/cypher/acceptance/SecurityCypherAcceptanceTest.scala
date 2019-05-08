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
import org.neo4j.internal.kernel.api.security.{AuthSubject, AuthenticationResult}
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.{BasicPasswordPolicy, CommunitySecurityModule, SecureHasher, SecurityTestUtils}
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.server.security.systemgraph.{BasicSystemGraphRealm, ContextSwitchingSystemGraphQueryExecutor}

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
  private val neo4jUser = user("neo4j", Seq("admin"))

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
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

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
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(
      Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"),
      foo ++ Map("member" -> "Bar"),
      foo ++ Map("member" -> "Baz")
    ))
  }

  test("should list privileges for all users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL PRIVILEGES")

    // THEN
    val grantGraph = Map("grant" -> "GRANT", "resource" -> "graph", "database" -> "*", "labels" -> Seq())
    val grantSchema = Map("grant" -> "GRANT", "resource" -> "schema", "database" -> "*", "labels" -> Seq())
    val grantToken = Map("grant" -> "GRANT", "resource" -> "token", "database" -> "*", "labels" -> Seq())
    val grantSystem = Map("grant" -> "GRANT", "resource" -> "system", "database" -> "*", "labels" -> Seq())
    val expected = Set(
      grantGraph ++ Map("action" -> "find", "role" -> "reader"),
      grantGraph ++ Map("action" -> "read", "role" -> "reader"),

      grantGraph ++ Map("action" -> "find", "role" -> "editor"),
      grantGraph ++ Map("action" -> "read", "role" -> "editor"),
      grantGraph ++ Map("action" -> "write", "role" -> "editor"),

      grantGraph ++ Map("action" -> "find", "role" -> "publisher"),
      grantGraph ++ Map("action" -> "read", "role" -> "publisher"),
      grantGraph ++ Map("action" -> "write", "role" -> "publisher"),
      grantToken ++ Map("action" -> "write", "role" -> "publisher"),

      grantGraph ++ Map("action" -> "find", "role" -> "architect"),
      grantGraph ++ Map("action" -> "read", "role" -> "architect"),
      grantGraph ++ Map("action" -> "write", "role" -> "architect"),
      grantToken ++ Map("action" -> "write", "role" -> "architect"),
      grantSchema ++ Map("action" -> "write", "role" -> "architect"),

      grantGraph ++ Map("action" -> "find", "role" -> "admin"),
      grantGraph ++ Map("action" -> "read", "role" -> "admin"),
      grantGraph ++ Map("action" -> "write", "role" -> "admin"),
      grantSystem ++ Map("action" -> "write", "role" -> "admin"),
      grantToken ++ Map("action" -> "write", "role" -> "admin"),
      grantSchema ++ Map("action" -> "write", "role" -> "admin"),
    )

    result.toSet should be(expected)
  }

  test("should list privileges for specific role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ROLE editor PRIVILEGES")

    // THEN
    val grantGraph = Map("grant" -> "GRANT", "resource" -> "graph", "database" -> "*", "labels" -> Seq())
    val expected = Set(
      grantGraph ++ Map("action" -> "find", "role" -> "editor"),
      grantGraph ++ Map("action" -> "read", "role" -> "editor"),
      grantGraph ++ Map("action" -> "write", "role" -> "editor")
    )

    result.toSet should be(expected)
  }

  test("should grant traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")

    // WHEN
    val result = execute("SHOW ROLE custom PRIVILEGES")

    // THEN
    val grantGraph = Map("grant" -> "GRANT", "resource" -> "graph", "database" -> "*", "labels" -> Seq("*"))
    val expected = Set(
      grantGraph ++ Map("action" -> "find", "role" -> "custom")
    )

    result.toSet should be(expected)
  }

  test("should grant traversal privilege to custom role for all databases but only a specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // WHEN
    val result = execute("SHOW ROLE custom PRIVILEGES")

    // THEN
    val grantGraph = Map("grant" -> "GRANT", "resource" -> "graph", "database" -> "*", "labels" -> Seq("A"))
    val expected = Set(
      grantGraph ++ Map("action" -> "find", "role" -> "custom")
    )

    result.toSet should be(expected)
  }

  test("should grant traversal privilege to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")

    // WHEN
    val result = execute("SHOW ROLE custom PRIVILEGES")

    // THEN
    val grantGraph = Map("grant" -> "GRANT", "resource" -> "graph", "database" -> "foo", "labels" -> Seq("A"))
    val expected = Set(
      grantGraph ++ Map("action" -> "find", "role" -> "custom")
    )

    result.toSet should be(expected)
  }

  test("should grant traversal privilege to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO custom")

    // WHEN
    val result = execute("SHOW ROLE custom PRIVILEGES")

    // THEN
    val grantGraph = Map("grant" -> "GRANT", "resource" -> "graph", "database" -> "foo", "labels" -> Seq("*"))
    val expected = Set(
      grantGraph ++ Map("action" -> "find", "role" -> "custom")
    )

    result.toSet should be(expected)
  }

  test("should grant traversal privilege to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES B (*) TO custom")

    // WHEN
    val result = execute("SHOW ROLE custom PRIVILEGES")

    // THEN
    val grantGraph = Map("grant" -> "GRANT", "resource" -> "graph", "database" -> "foo", "labels" -> Seq("A", "B"))
    val expected = Set(
      grantGraph ++ Map("action" -> "find", "role" -> "custom")
    )

    result.toSet should be(expected)
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

    try {
      // WHEN
      execute("CREATE ROLE foo")

      fail("Expected error \"The specified role 'foo' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("The specified role 'foo' already exists.")
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

    try {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("Cannot create role 'bar' from non-existent role 'foo'.")
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

    try {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"The specified role 'bar' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("The specified role 'bar' already exists.")
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

    try {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("Cannot create role 'bar' from non-existent role 'foo'.")
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
      case e: Exception => e.getMessage should be("Role 'foo' does not exist.")
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
    result.toSet should be(Set(neo4jUser))
  }

  test("should list all users") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   : dragon, fairy
    // Baz   :
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet shouldBe Set(neo4jUser, user("Bar"), user("Baz"), user("Zet"))
  }

  test("should assign roles and list users with roles") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   : dragon, fairy
    // Baz   :
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")
    execute("CREATE ROLE dragon")
    execute("CREATE ROLE fairy")
    execute("GRANT ROLE dragon TO Bar")
    execute("GRANT ROLE fairy TO Bar")
    execute("GRANT ROLE fairy TO Zet")
    //TODO: execute("GRANT ROLE fairy TO Bar, Zet")

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet shouldBe Set(neo4jUser, user("Bar", Seq("fairy", "dragon")), user("Baz"), user("Zet", Seq("fairy")))
  }

  test("should create user with password as string") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    // WHEN
    execute("CREATE USER bar SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with mixed password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    // WHEN
    execute("CREATE USER bar SET PASSWORD 'p4s5W*rd'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("bar"))
    testUserLogin("bar", "p4s5w*rd", AuthenticationResult.FAILURE)
    testUserLogin("bar", "PASSword", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.FAILURE)
    testUserLogin("bar", "p4s5W*rd", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // WHEN
    execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> "bar"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo"))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail to create user with numeric password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    try {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> 123))

      fail("Expected error \"Only string values are accepted as password, got: Integer\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterWrongTypeException => e.getMessage should be("Only string values are accepted as password, got: Integer")
    }
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail to create user with password as missing parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    try {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED")

      fail("Expected error \"Expected parameter(s): password\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterNotFoundException => e.getMessage should be("Expected parameter(s): password")
    }
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail to create user with password as null parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    try {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> null))

      fail("Expected error \"Expected parameter(s): password\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterNotFoundException => e.getMessage should be("Expected parameter(s): password")
    }
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should create user with password change not required") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", passwordChangeRequired = false))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.SUCCESS)
  }

  test("should create user with status active") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo"))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with status suspended") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", suspended = true))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.FAILURE)
  }

  test("should create user with all parameters") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED SET STATUS SUSPENDED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", passwordChangeRequired = false, suspended = true))
  }

  test("should fail on creating already existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // WHEN
    try {
      execute("CREATE USER neo4j SET PASSWORD 'password'")

      fail("Expected error \"The specified user 'neo4j' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("The specified user 'neo4j' already exists.")
    }

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should drop user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should fail on dropping non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    try {
      // WHEN
      execute("DROP USER foo")

      fail("Expected error \"User 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("User 'foo' does not exist.")
    }

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should alter user password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz'")

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user password with mixed upper- and lowercase letters") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'bAz'")

    // THEN
    testUserLogin("foo", "bAz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
    testUserLogin("foo", "BaZ", AuthenticationResult.FAILURE)
    testUserLogin("foo", "BAZ", AuthenticationResult.FAILURE)
  }

  test("should alter user password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail on altering user password as missing parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

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
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should alter user status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user password and mode") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' CHANGE NOT REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should alter user password and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo","bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should alter user password mode and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user on all points as suspended") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should alter user on all points as active") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should fail on alter user password as list parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    try {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> Seq("baz", "boo")))

      fail("Expected error \"Only string values are accepted as password, got: List\" but succeeded.")
    } catch {
      // THEN
      case e: ParameterWrongTypeException => e.getMessage should be("Only string values are accepted as password, got: List")
    }
  }

  private def user(username: String, roles: Seq[String] = Seq.empty, suspended: Boolean = false, passwordChangeRequired: Boolean = true) = {
    Map("user" -> username, "roles" -> roles, "suspended" -> suspended, "passwordChangeRequired" -> passwordChangeRequired)
  }

  private def testUserLogin(username: String, password: String, expected: AuthenticationResult): Unit = {
    val login = systemGraphRealm.login(SecurityTestUtils.authToken(username, password))
    val result = login.subject().getAuthenticationResult
    if (expected == AuthenticationResult.FAILURE && result != AuthenticationResult.FAILURE) {
      // This is a hack to get around the fact that login() does not fail for suspended users
      val user = systemGraphRealm.getUser(username)
      user.hasFlag(BasicSystemGraphRealm.IS_SUSPENDED) should equal(true)
    }
    else {
      result should be(expected)
    }
  }

  private def prepareUser(username: String, password: String): Unit = {
    execute(s"CREATE USER $username SET PASSWORD '$password'")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo"))
    testUserLogin(username, "wrong", AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
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
    when(config.get(SecuritySettings.property_level_authorization_enabled)).thenReturn(false)
    when(config.get(SecuritySettings.auth_cache_ttl)).thenReturn(Duration.ZERO)
    when(config.get(SecuritySettings.auth_cache_max_capacity)).thenReturn(10)
    when(config.get(SecuritySettings.auth_cache_use_ttl)).thenReturn(true)
    when(config.get(SecuritySettings.security_log_successful_authentication)).thenReturn(false)
    when(config.get(GraphDatabaseSettings.auth_max_failed_attempts)).thenReturn(3) //!
    when(config.get(GraphDatabaseSettings.auth_lock_time)).thenReturn(Duration.ofSeconds(5))
    when(config.get(SecuritySettings.auth_providers)).thenReturn(List(SecuritySettings.NATIVE_REALM_NAME).asJava)

    systemGraphRealm = new SystemGraphRealm( // this is also a UserManager even if the Name does not indicate that
      systemGraphOperations,
      systemGraphInitializer,
      false,
      secureHasher,
      new BasicPasswordPolicy(),
      CommunitySecurityModule.createAuthenticationStrategy(config),
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
