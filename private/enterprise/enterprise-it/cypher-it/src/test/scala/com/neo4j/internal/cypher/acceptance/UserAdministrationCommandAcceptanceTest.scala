/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.CacheCounts
import org.neo4j.cypher.ExecutionEngineCacheCounter
import org.neo4j.cypher.internal.DatabaseStatus.Online
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.JavaConverters.mapAsJavaMapConverter

class UserAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("GraphStatistics should tell us if a query contains system updates or not"){
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n) RETURN n").queryStatistics().containsUpdates() should be(true)
    execute("CREATE (n) RETURN n").queryStatistics().containsSystemUpdates() should be(false)
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").queryStatistics().containsSystemUpdates() should be(false)
    execute("CREATE USER foo SET PASSWORD 'bar'").queryStatistics().containsSystemUpdates() should be(true)
  }

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "CREATE USER Bar SET PASSWORD 'neo'" -> 1,
      "CREATE USER Baz IF NOT EXISTS SET PASSWORD 'neo'" -> 1,
      "CREATE OR REPLACE USER Bar SET PASSWORD 'neo'" -> 2,
      "CREATE OR REPLACE USER Bao SET PASSWORD 'neo'" -> 1,
      "ALTER USER Bar SET PASSWORD 'neo4j' CHANGE NOT REQUIRED" -> 1,
      "DROP USER Bar" -> 1,
      "DROP USER Baz IF EXISTS" -> 1
    ))
  }

  // Tests for showing users

  test("should show default user") {
    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet should be(Set(neo4jUser))
  }

  test("should show all users") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   :
    // Baz   :
    // Zet   :
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
    // WHEN
    execute("CREATE USER bar SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user using if not exists") {
    // WHEN
    execute("CREATE USER bar IF NOT EXISTS SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with mixed password") {
    // WHEN
    execute("CREATE USER bar SET PASSWORD 'p4s5W*rd'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("bar"))
    testUserLogin("bar", "p4s5w*rd", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.FAILURE)
    testUserLogin("bar", "p4s5W*rd", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with empty password") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should create user with password as parameter") {
    // WHEN
    execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> "bar"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo"))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with username and password as parameter") {
    // WHEN
    execute("CREATE USER $user SET PASSWORD $password CHANGE REQUIRED", Map("user" -> "foo", "password" -> "bar"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo"))
    testUserLogin("user", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should not use query cache when creating multiple users with parameterized passwords") {
    // GIVEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    val passwords = Seq("bar", "abc", "password")
    val createCount = Map("cachable" -> 1, "total" ->  2)   // create has one outer and one inner command
    val dropCount = Map("cachable" -> 3, "total" ->  3)     // drop has one outer and two inner commands
    val hits = (createCount("cachable") + dropCount("cachable")) * (passwords.size - 1)
    val total = (createCount("total") + dropCount("total")) * passwords.size
    val misses = total - hits

    // WHEN
    val counter = new ExecutionEngineCacheCounter()
    kernelMonitors.addMonitorListener(counter)
    counter.counts should equal(CacheCounts())
    passwords.foreach { pw =>
        execute("CREATE USER foo SET PASSWORD $password CHANGE NOT REQUIRED", Map("password" -> pw))
        testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
        testUserLogin("foo", pw, AuthenticationResult.SUCCESS)
        execute("DROP USER foo")
        testUserLogin("foo", pw, AuthenticationResult.FAILURE)
    }

    // THEN
    counter.counts should equal(CacheCounts(misses = misses, hits = hits), compilations = commandCount)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should use query cache when creating multiple roles with parameterized names") {
    // GIVEN
    execute("SHOW ROLES").toList.size shouldBe defaultRoles.size
    val commandCount = 5

    // WHEN
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)
    counter.counts should equal(CacheCounts())
    Range(0, commandCount).foreach { index =>
      execute("CREATE ROLE $role", Map("role" -> s"Role$index"))
    }

    // THEN
    counter.counts should equal(CacheCounts(misses = 2, hits = (commandCount - 1) * 2))
    execute("SHOW ROLES").toList.size should be(defaultRoles.size + commandCount)
  }

  test("should fail when creating user with numeric password as parameter") {
    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> 123))
      // THEN
    } should have message "Only string values are accepted as password, got: Integer"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail when creating user with password as missing parameter") {
    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED")
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail when creating user with password as null parameter") {
    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> null))
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should create user with password change not required") {
    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", passwordChangeRequired = false))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.SUCCESS)
  }

  test("should create user with status active") {
    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo"))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with status suspended") {
    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", suspended = true))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.FAILURE)
  }

  test("should create user with all parameters") {
    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED SET STATUS SUSPENDED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", passwordChangeRequired = false, suspended = true))
  }

  test("should fail when creating already existing user") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER neo4j SET PASSWORD 'password'")
      // THEN
    } should have message "Failed to create the specified user 'neo4j': User already exists."

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER $user SET PASSWORD 'password'", Map("user" -> "neo4j"))
      // THEN
    } should have message "Failed to create the specified user 'neo4j': User already exists."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should do nothing when creating already existing user using if not exists") {
    // WHEN
    execute("CREATE USER neo4j IF NOT EXISTS SET PASSWORD 'password' CHANGE NOT REQUIRED SET STATUS SUSPENDED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
  }

  test("should fail when creating user with illegal username") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER `` SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED")
      // THEN
    } should have message "The provided username is empty."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // and using parameter
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER $user SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED", Map("user" -> ""))
      // THEN
    } should have message "The provided username is empty."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // and with invalid username
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER `neo:4j` SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED")
      // THEN
    } should have message
      """Username 'neo:4j' contains illegal characters.
        |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    // and with invalid username as parameter
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER $user SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED", Map("user" -> "neo:4j"))
      // THEN
    } should have message
      """Username 'neo:4j' contains illegal characters.
        |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)

    val exception = the[SyntaxException] thrownBy {
      // WHEN
      execute("CREATE USER `3neo4j` SET PASSWORD 'password'")
      execute("CREATE USER 4neo4j SET PASSWORD 'password'")
    }
    // THEN
    exception.getMessage should include("Invalid input '4'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("3neo4j"))
  }

  test("should replace existing user") {
    // WHEN: creation
    execute("CREATE OR REPLACE USER bar SET PASSWORD 'firstPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "firstPassword", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)

    // WHEN: replacing
    execute("CREATE OR REPLACE USER bar SET PASSWORD 'secondPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("bar"))
    testUserLogin("bar", "firstPassword", AuthenticationResult.FAILURE)
    testUserLogin("bar", "secondPassword", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when replacing current user") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("neo4j", "bar", "CREATE OR REPLACE USER neo4j SET PASSWORD 'baz'")
      // THEN
    } should have message "Failed to replace the specified user 'neo4j': Deleting yourself is not allowed."

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("neo4j", "bar", "CREATE OR REPLACE USER $user SET PASSWORD 'baz'", Map[String, Object]("user" -> "neo4j").asJava)
      // THEN
    } should have message "Failed to replace the specified user 'neo4j': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive)
    testUserLogin("neo4j", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("neo4j", "baz", AuthenticationResult.FAILURE)
  }

  test("should get syntax exception when using both replace and if not exists") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'pass'")
    }

    // THEN
    exception.getMessage should include("Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")

    // WHEN
    val exception2 = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE USER $user IF NOT EXISTS SET PASSWORD 'pass'", Map("user" -> "foo"))
    }

    // THEN
    exception2.getMessage should include("Failed to create the specified user '$user': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
  }

  // Tests for dropping users

  test("should drop user") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should drop existing user using if exists") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should re-create dropped user") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("DROP USER foo")

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'bar'")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("foo")))
  }

  test("should be able to drop the user that created you") {
    // GIVEN
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE admin TO alice")

    // WHEN
    executeOnSystem("alice", "abc", "CREATE USER bob SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    executeOnSystem("alice", "abc", "GRANT ROLE admin to bob")

    // THEN
    execute("SHOW USERS").toSet should be(Set(
      neo4jUser,
      user("alice", Seq("admin"), passwordChangeRequired = false),
      user("bob", Seq("admin"), passwordChangeRequired = false)
    ))

    // WHEN
    executeOnSystem("bob", "bar",  "DROP USER alice")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("bob", Seq("admin"), passwordChangeRequired = false)))
  }

  test("should fail when dropping current user that is admin") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("neo4j", "neo", "DROP USER neo4j")
      // THEN
    } should have message "Failed to delete the specified user 'neo4j': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive)

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("neo4j", "neo", "DROP USER $user IF EXISTS", Map[String, Object]("user" -> "neo4j").asJava)
      // THEN
    } should have message "Failed to delete the specified user 'neo4j': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive)
  }

  test("should fail when dropping non-existing user") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER foo")
      // THEN
    } should have message "Failed to delete the specified user 'foo': User does not exist."

    // and with parameter
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER $user", Map("user" -> "foo"))
      // THEN
    } should have message "Failed to delete the specified user 'foo': User does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER `:foo`")
      // THEN
    } should have message "Failed to delete the specified user ':foo': User does not exist."

    // and an invalid (non-existing) one with parameter
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER $user", Map("user" -> ":foo"))
      // THEN
    } should have message "Failed to delete the specified user ':foo': User does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should do nothing when dropping non-existing user using if exists") {
    // WHEN
    execute("DROP USER foo IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))

    // and an invalid (non-existing) one

    // WHEN
    execute("DROP USER `:foo` IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  // Tests for altering users

  test("should alter user password") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz'")

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user password with mixed upper- and lowercase letters") {
    // GIVEN
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
    prepareUser("foo", "bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD 'bar'", Map("user" -> "foo"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': Old password and new password cannot be the same."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with empty password parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD $password", Map("user" -> "foo", "password" -> ""))
      // THEN
    } should have message "A password cannot be empty."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with current password parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password", Map("password" -> "bar"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': Old password and new password cannot be the same."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter user password as parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail when alter user password as list parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> Seq("baz", "boo")))
      // THEN
    } should have message "Only string values are accepted as password, got: List"
  }

  test("should fail when alter user password as string and parameter") {
    // GIVEN
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
    prepareUser("foo", "bar")

    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password")
      // THEN
    } should have message "Expected parameter(s): password"
  }

  test("should alter user password mode") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should alter user password mode to change required") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should give correct error message when user with password change required tries to execute a query") {
    // GIVEN
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE admin TO alice")

    // WHEN
    executeOnSystem("alice", "abc", "ALTER USER alice SET PASSWORD CHANGE REQUIRED")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("alice", "abc", "MATCH (n) RETURN n")
    } should have message String.format("Permission denied." + PASSWORD_CHANGE_REQUIRED_MESSAGE)
  }

  // TODO this is a hot fix to get browser to get password change required error
  // It should be changed so that only a few key procedures are allowed to be run with password change required
  test("should give correct error message when user with password change required tries to execute db.indexes") {
    // GIVEN
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE admin TO alice")

    // WHEN
    executeOnSystem("alice", "abc", "ALTER USER alice SET PASSWORD CHANGE REQUIRED")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("alice", "abc", "CALL db.indexes")
    } should have message String.format("Permission denied." + PASSWORD_CHANGE_REQUIRED_MESSAGE)
  }

  test("should alter user status to suspended") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user status to active") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET STATUS ACTIVE")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should suspend a suspended user") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("ALTER USER foo SET STATUS SUSPENDED")
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)

    // WHEN
    execute("ALTER USER foo SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should not alter current user status to suspended") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'potato' CHANGE NOT REQUIRED")

    // WHEN
    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      executeOnSystem("neo4j", "potato", "ALTER USER neo4j SET STATUS SUSPENDED")
    } should have message "Failed to alter the specified user 'neo4j': Changing your own activation status is not allowed."

    // THEN
    testUserLogin("neo4j", "potato", AuthenticationResult.SUCCESS)
  }

  test("should not alter current user status to active") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'potato' CHANGE NOT REQUIRED")

    // WHEN
    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      executeOnSystem("neo4j", "potato", "ALTER USER $user SET STATUS ACTIVE", Map[String, Object]("user" -> "neo4j").asJava)
    } should have message "Failed to alter the specified user 'neo4j': Changing your own activation status is not allowed."

    // THEN
    testUserLogin("neo4j", "potato", AuthenticationResult.SUCCESS)
  }

  test("should alter user password and mode") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' CHANGE NOT REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should alter user password as parameter and password mode") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password CHANGE NOT REQUIRED", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should alter user password and status") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should alter user password as parameter and status") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter user password mode and status") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user on all points as suspended") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should alter user on all points as active") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should alter user on all points as active with parameter password") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should fail when altering a non-existing user: string password") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz'")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD 'baz'", Map("user" -> "foo"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: parameter password (and illegal username)") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER `neo:4j` SET PASSWORD $password", Map("password" -> "baz"))
      // THEN
    } should have message "Failed to alter the specified user 'neo:4j': User does not exist."
  }

  test("should fail when altering a non-existing user: string password and password mode") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz' CHANGE NOT REQUIRED")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: parameter password and password mode") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE REQUIRED", Map("password" -> "baz"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: string password and status") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz' SET STATUS ACTIVE")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: parameter password and status") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD $password SET STATUS ACTIVE", Map("user" -> "foo", "password" -> "baz"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: string password, password mode and status") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz' CHANGE REQUIRED SET STATUS ACTIVE")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: password mode") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: password mode and status") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD CHANGE REQUIRED SET STATUS SUSPENDED")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing user: status") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET STATUS SUSPENDED")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a non-existing parameterized user: status") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET STATUS SUSPENDED", Map("user" -> "foo"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail when altering a dropped user") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("DROP USER foo")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password SET STATUS ACTIVE", Map("password" -> "baz"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  // Tests for changing own password

  test("should change own password") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should change own password to password with mixed upper- and lowercase letters and characters") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO '!bAr%'")

    // THEN
    testUserLogin("foo", "!bAr%", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "!bar%", AuthenticationResult.FAILURE)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should change own password when password change is required") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("foo", Seq("editor"), passwordChangeRequired = false))
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should change own password when user has no role") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail on changing own password from wrong password") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'wrongPassword' TO 'baz'")
      // THEN
    } should have message "User 'foo' failed to alter their own password: Invalid principal or credentials."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password to invalid password") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO ''")
      // THEN
    } should have message "A password cannot be empty."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'bar'")
      // THEN
    } should have message "User 'foo' failed to alter their own password: Old password and new password cannot be the same."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should change own password to parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "baz")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password", params = parameter)

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password to list parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    val passwordList = new util.ArrayList[String]()
    passwordList.add("baz")
    passwordList.add("boo")
    parameter.put("password", passwordList)

    the[QueryExecutionException] thrownBy { // the ParameterWrongTypeException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password", params = parameter)
      // THEN
    } should have message "Only string values are accepted as password, got: List"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password to missing parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password")
      // THEN
    } should have message "Expected parameter(s): password"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should change own password from parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "bar")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $password TO 'baz'", params = parameter)

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from integer parameter") {
    // GIVEN
    prepareUser("foo", "123")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", Integer.valueOf(123))

    the[QueryExecutionException] thrownBy { // the ParameterWrongTypeException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "123", "ALTER CURRENT USER SET PASSWORD FROM $password TO 'bar'", params = parameter)
      // THEN
    } should have message "Only string values are accepted as password, got: Integer"

    // THEN
    testUserLogin("foo", "123", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password from missing parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $password TO 'baz'")
      // THEN
    } should have message "Expected parameter(s): password"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should change own password from parameter to parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("currentPassword", "bar")
    parameter.put("newPassword", "baz")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword", params = parameter)

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail to change own password from parameter to parameter with same value") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("currentPassword", "bar")
    parameter.put("newPassword", "bar")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword", params = parameter)
      // THEN
    } should have message "User 'foo' failed to alter their own password: Old password and new password cannot be the same."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should fail to change own password from parameter to same parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("currentPassword", "bar")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $currentPassword", params = parameter)
      // THEN
    } should have message "User 'foo' failed to alter their own password: Old password and new password cannot be the same."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from wrong password parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("wrongPassword", "boo")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $wrongPassword TO 'baz'", params = parameter)
      // THEN
    } should have message "User 'foo' failed to alter their own password: Invalid principal or credentials."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from wrong password parameter to password parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("wrongPassword", "boo")
    parameter.put("newPassword", "baz")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $wrongPassword TO $newPassword", params = parameter)
      // THEN
    } should have message "User 'foo' failed to alter their own password: Invalid principal or credentials."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from wrong password to password parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("newPassword", "baz")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'boo' TO $newPassword", params = parameter)
      // THEN
    } should have message "User 'foo' failed to alter their own password: Invalid principal or credentials."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from existing parameter to missing parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("currentPassword", "bar")

    the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword", params = parameter)
      // THEN
    } should have message "Expected parameter(s): newPassword"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password from missing parameter to existing parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("newPassword", "baz")

    the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword", params = parameter)
      // THEN
    } should have message "Expected parameter(s): currentPassword"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password from parameter to parameter when both are missing") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val e = the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword")
    }
    // THEN
    e.getMessage should (be("Expected parameter(s): newPassword, currentPassword") or be("Expected parameter(s): currentPassword, newPassword"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password to string and parameter") {
    // GIVEN
    prepareUser("foo", "bar")
    execute("GRANT ROLE editor TO foo")
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "imAParameter")

    // WHEN
    val exception = the[QueryExecutionException] thrownBy { // the Syntax exception gets wrapped
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'imAString'+$password", params = parameter)
    }
    // THEN
    exception.getMessage should include("Invalid input '+': expected whitespace, ';' or end of input")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password when AUTH DISABLED") {
    the[IllegalStateException] thrownBy {
      // WHEN
      execute("ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'")
      // THEN
    } should have message "User failed to alter their own password: Command not available with auth disabled."
  }

  // Tests for user administration with restricted privileges

  test("should fail create user for when password change required") {
    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("neo4j", "neo4j", "CREATE USER bob SET PASSWORD 'builder' CHANGE NOT REQUIRED")
      // THEN
    } should have message String.format(PERMISSION_DENIED + PASSWORD_CHANGE_REQUIRED_MESSAGE)

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser))
  }

  test("should fail create user for user with editor role") {
    // GIVEN
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO alice")

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
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE USER bob SET PASSWORD 'builder'")
    execute("GRANT ROLE editor TO alice")

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
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE USER bob SET PASSWORD 'builder'")
    execute("GRANT ROLE editor TO alice")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "ALTER USER bob SET STATUS SUSPENDED")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false), user("bob")))
  }

  test("should allow alter own user password without admin only through 'ALTER CURRENT USER SET PASSWORD' command") {
    // GIVEN
    execute("CREATE USER alice SET PASSWORD 'abc'")
    execute("GRANT ROLE editor TO alice")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "ALTER USER alice SET PASSWORD 'xyz' CHANGE NOT REQUIRED")
      // THEN
    } should have message PERMISSION_DENIED

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"))))

    // WHEN
    executeOnSystem("alice", "abc", "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO 'xyz'")

    // THEN
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("alice", Seq("editor"), passwordChangeRequired = false)))
  }

  test("should fail alter own user status without admin") {
    // GIVEN
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO alice")

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
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED SET STATUS SUSPENDED")
    execute("GRANT ROLE admin TO alice")

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
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO alice")

    // WHEN / THEN
    executeOnSystem("alice", "abc", s"SHOW DATABASE $DEFAULT_DATABASE_NAME",
      resultHandler = (row, _) => row.get("name").equals(DEFAULT_DATABASE_NAME))
  }

  test("should allow show default database for non admin user") {

    // GIVEN
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO alice")

    // WHEN / THEN
    executeOnSystem("alice", "abc", "SHOW DEFAULT DATABASE",
      resultHandler = (row, _) => {
        row.get("name").equals(DEFAULT_DATABASE_NAME)
        row.get("requestedStatus").equals(Online.stringValue())
      })
  }

  // helper methods

  private def prepareUser(username: String, password: String): Unit = {
    execute(s"CREATE USER $username SET PASSWORD '$password'")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user(username))
    testUserLogin(username, "wrong", AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }
}
