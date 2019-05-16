/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher._
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.server.security.auth.SecurityTestUtils

class UserManagementDDLAcceptanceTest extends DDLAcceptanceTestBase {
  private val neo4jUser = user("neo4j", Seq("admin"))

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
    // Bar   :
    // Baz   :
    // Zet   :
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet shouldBe Set(neo4jUser, user("Bar"), user("Baz"), user("Zet"))
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
    prepareUser("foo", "bar")

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
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val result = login.subject().getAuthenticationResult
    result should be(expected)
  }

  private def prepareUser(username: String, password: String): Unit = {
    execute(s"CREATE USER $username SET PASSWORD '$password'")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user(username))
    testUserLogin(username, "wrong", AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }
}
