/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher._
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.server.security.auth.SecurityTestUtils
import org.parboiled.errors.ParserRuntimeException

import scala.collection.Map

class UserManagementDDLAcceptanceTest extends DDLAcceptanceTestBase {
  private val neo4jUser = user("neo4j", Seq("admin"))

  // Tests for showing users

  test("should show default user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet should be(Set(neo4jUser))
  }

  test("should fail when showing users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW USERS")
      // THEN
    } should have message "Trying to run `SHOW USERS` against non-system database."
  }

  test("should show all users") {
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

  // Tests for creating users

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

  test("should fail when creating user with empty password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
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

  test("should fail when creating user with numeric password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> 123))
      // THEN
    } should have message "Only string values are accepted as password, got: Integer"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail when creating user with password as missing parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED")
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail when creating user with password as null parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> null))
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail when creating user when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD 'bar'")
      // THEN
    } should have message "Trying to run `CREATE USER` against non-system database."
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

  test("should fail when creating already existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER neo4j SET PASSWORD 'password'")
      // THEN
    } should have message "The specified user 'neo4j' already exists."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail when creating user with illegal username") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    val exceptionIllegalCharacter = the[ParserRuntimeException] thrownBy {
      // WHEN
      execute("CREATE USER `neo:4j` SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED")
    }
    // THEN
    exceptionIllegalCharacter.getMessage should include("Username 'neo:4j' contains illegal characters.")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    val exceptionUnescaped = the[SyntaxException] thrownBy {
      // WHEN
      execute("CREATE USER `3neo4j` SET PASSWORD 'password'")
      execute("CREATE USER 4neo4j SET PASSWORD 'password'")
    }
    // THEN
    exceptionUnescaped.getMessage should include("Invalid input '4'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("3neo4j"))
  }

  // Tests for dropping users

  test("should drop user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should re-create dropped user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")
    execute("DROP USER foo")
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'bar'")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("foo")))
  }

  test("should fail when dropping current user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin"), passwordChangeRequired = false))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      executeOnSystem("neo4j", "neo", "DROP USER neo4j")
      // THEN
    } should have message "Deleting yourself (user 'neo4j') is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin"), passwordChangeRequired = false))
  }

  test("should fail when dropping non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER foo")
      // THEN
    } should have message "User 'foo' does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER `:foo`")
      // THEN
    } should have message "User ':foo' does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should fail when dropping user when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("DROP USER foo")
      // THEN
    } should have message "Trying to run `DROP USER` against non-system database."
  }

  // Tests for altering users

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

  test("should fail when alter user with invalid password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'bar'")
      // THEN
    } should have message "Old password and new password cannot be the same."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
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

  test("should fail when alter user password as list parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> Seq("baz", "boo")))
      // THEN
    } should have message "Only string values are accepted as password, got: List"
  }

  test("should fail when alter user password as string and parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    val exception = the[SyntaxException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'imAString'+$password", Map("password" -> "imAParameter"))
      // THEN
    }
    exception.getMessage should include("Invalid input '+': expected whitespace, SET, ';' or end of input")
  }

  test("should fail when altering user password as missing parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password")
      // THEN
    } should have message "Expected parameter(s): password"
  }

  test("should fail when altering user when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'bar'")
      // THEN
    } should have message "Trying to run `ALTER USER` against non-system database."
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

  test("should alter user status to suspended") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user status to active") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET STATUS ACTIVE")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should suspend a suspended user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")
    execute("ALTER USER foo SET STATUS SUSPENDED")
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)

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

  test("should alter user password as parameter and password mode") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password CHANGE NOT REQUIRED", Map("password" -> "baz"))

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

  test("should alter user password as parameter and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
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
    execute("ALTER USER foo SET PASSWORD 'baz' SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should alter user on all points as active with parameter password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should fail when altering a non-existing user: string password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz'")
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: parameter password (and illegal username)") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER `neo:4j` SET PASSWORD $password", Map("password" -> "baz"))
      // THEN
    } should have message "User 'neo:4j' does not exist."
  }

  test("should fail when altering a non-existing user: string password and password mode") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz' CHANGE NOT REQUIRED")
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: parameter password and password mode") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE REQUIRED", Map("password" -> "baz"))
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: string password and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz' SET STATUS ACTIVE")
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: parameter password and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> "baz"))
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: string password, password mode and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz' CHANGE REQUIRED SET STATUS ACTIVE")
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: password mode") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: password mode and status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD CHANGE REQUIRED SET STATUS SUSPENDED")
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a non-existing user: status") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET STATUS SUSPENDED")
      // THEN
    } should have message "User 'foo' does not exist."
  }

  test("should fail when altering a dropped user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("DROP USER foo")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> "baz"))
      // THEN
    } should have message "User 'foo' does not exist."
  }

  ignore("should allow create user for user with only admin privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED")
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE customAdmin")
    execute("GRANT ROLE customAdmin TO alice")
    // TODO currently only way to grant admin privilege, should be replaced by proper DDL command when available
    executeOnDefault("neo4j", "neo4j", "CALL dbms.security.grantPrivilegeToRole('customAdmin', 'write', 'system')")

    // WHEN
    executeOnSystem("alice", "abc", "CREATE USER bob SET PASSWORD 'builder'")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("customAdmin"), passwordChangeRequired = false), user("bob")))
  }

  test("should fail create user for when password change required") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("neo4j", "neo4j", "CREATE USER bob SET PASSWORD 'builder' CHANGE NOT REQUIRED")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should fail create user for user with editor role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO alice")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false)))

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "CREATE USER bob SET PASSWORD 'builder' CHANGE NOT REQUIRED")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false)))
  }

  test("should fail drop user for user with editor role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE USER bob SET PASSWORD 'builder'")
    execute("GRANT ROLE editor TO alice")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false), user("bob")))

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "DROP USER bob")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false), user("bob")))
  }

  test("should fail alter other user for user with editor role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE USER bob SET PASSWORD 'builder'")
    execute("GRANT ROLE editor TO alice")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false), user("bob")))

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "ALTER USER bob SET STATUS SUSPENDED")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false), user("bob")))
  }

  ignore("should allow alter own user password without admin") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc'")
    execute("GRANT ROLE editor TO alice")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"))))

    // WHEN
    executeOnSystem("alice", "abc", "ALTER USER alice SET PASSWORD 'xyz' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false)))
  }

  test("should fail alter own user status without admin") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO alice")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false)))

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "ALTER USER alice SET STATUS SUSPENDED")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false)))
  }

  test("should fail alter own user status when suspended") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED SET STATUS SUSPENDED")
    execute("GRANT ROLE admin TO alice")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("admin"), suspended = true, passwordChangeRequired = false)))

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "ALTER USER alice SET STATUS ACTIVE")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("admin"), suspended = true, passwordChangeRequired = false)))
  }

  test("should allow show database for non admin user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO alice")

    // WHEN / THEN
    executeOnSystem("alice", "abc", "SHOW DATABASE neo4j", (row, _) => row.get("name").equals("neo4j"))
  }

  // helper methods

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
