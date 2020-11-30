/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.internal.kernel.api.security.AuthenticationResult

import scala.collection.JavaConverters.mapAsJavaMapConverter

class DBMSProceduresAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  private val alterDefaultUserQuery = s"ALTER USER $defaultUsername SET PASSWORD '$password' CHANGE NOT REQUIRED"

  test("should execute dbms.security.createUser on system") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    executeOnSystem(defaultUsername, password, s"CALL dbms.security.createUser('$username', '$password')")

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user(username))
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should explain dbms.security.createUser on system") {
    // WHEN
    val result = execute( s"EXPLAIN CALL dbms.security.createUser('$username', '$password')" )

    //THEN
    result.executionPlanDescription() should haveAsRoot.aPlan( "dbms.security.createUser" )
  }

  test("should execute dbms.security.createUser with return values on system") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    executeOnSystem(defaultUsername, password, s"CALL dbms.security.createUser('$username', '$password') RETURN 'yay'")

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user(username))
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should not execute dbms.security.createUser with more clauses on system") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem(defaultUsername, password,
        s"""MATCH (anything)
           |CALL dbms.security.createUser('$username', '$password')
           |RETURN anything""".stripMargin)
    }
    exception.getMessage should include("Not a recognised system command or procedure. This Cypher command can only be executed in a user database:")

    val exception2 = the[RuntimeException] thrownBy {
      executeOnSystem(defaultUsername, password,
        s"""MERGE (anything) WITH anything
           |CALL dbms.security.createUser('$username', '$password')
           |RETURN anything""".stripMargin)
    }
    exception2.getMessage should include("Not a recognised system command or procedure. This Cypher command can only be executed in a user database:")


    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive)
  }

  test("should execute dbms.security.createUser on system with username parameter") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    executeOnSystem(defaultUsername, password, s"CALL dbms.security.createUser($$name, '$password')", Map[String, Object]("name" -> username).asJava)

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user(username))
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should execute dbms.security.createUser on system with password parameter") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    executeOnSystem(defaultUsername, password, s"CALL dbms.security.createUser('$username', $$password)", Map[String, Object]("password" -> password).asJava)

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user(username))
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail dbms.security.createUser for existing user") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    val exception = the[QueryExecutionException] thrownBy {
      executeOnSystem(defaultUsername, password, s"CALL dbms.security.createUser('$defaultUsername', '$newPassword')")
    }
    // THEN
    exception.getMessage should include(s"Failed to create the specified user '$defaultUsername': User already exists.")
    testUserLogin(defaultUsername, password, AuthenticationResult.SUCCESS)
    testUserLogin(defaultUsername, newPassword, AuthenticationResult.FAILURE)
  }

  test("should fail using non-system procedure on system") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem(defaultUsername, password, "CALL db.createLabel('Foo')") // any procedure that we will never allow on system should be here
    }
    exception.getMessage should include("Not a recognised system command or procedure. This Cypher command can only be executed in a user database:")
  }

  test("should fail using non-existing procedure on system") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem(defaultUsername, password, "CALL dbms.something.something.profit()")
    }
    exception.getMessage should include("There is no procedure with the name `dbms.something.something.profit` registered for this database instance.")
  }

  Seq(
    (s"CALL dbms.security.createUser('$username', '$password')", "dbms.security.createUser"),
    (s"CALL dbms.security.changeUserPassword('$username', '$password')", "dbms.security.changeUserPassword"),
    (s"CALL dbms.security.addRoleToUser('$roleName', '$username')", "dbms.security.addRoleToUser"),
    (s"CALL dbms.security.removeRoleFromUser('$roleName', '$username')", "dbms.security.removeRoleFromUser"),
    (s"CALL dbms.security.deleteUser('$username')", "dbms.security.deleteUser"),
    (s"CALL dbms.security.suspendUser('$username')", "dbms.security.suspendUser"),
    (s"CALL dbms.security.activateUser('$username')", "dbms.security.activateUser"),
    ("CALL dbms.security.listUsers()", "dbms.security.listUsers"),
    ("CALL dbms.security.listRoles()", "dbms.security.listRoles"),
    (s"CALL dbms.security.listRolesForUser('Alice')", "dbms.security.listRolesForUser"),
    (s"CALL dbms.security.listUsersForRole('$roleName')", "dbms.security.listUsersForRole"),
    (s"CALL dbms.security.createRole('$roleName')", "dbms.security.createRole"),
    (s"CALL dbms.security.deleteRole('$roleName')", "dbms.security.deleteRole")
  ).foreach {
    case (query, name) =>
      test(s"should fail using $name on default database") {
        // GIVEN
        execute(alterDefaultUserQuery)

        // WHEN
        the[RuntimeException] thrownBy {
          executeOnDBMSDefault(defaultUsername, password, query)
        } should have message s"This is an administration command and it should be executed against the system database: $name"
      }
  }
}
