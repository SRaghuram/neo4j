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

  test("should execute dbms.security.createUser on system") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('Alice', 'foo')")

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user("Alice"))
    testUserLogin("Alice", "foo", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should explain dbms.security.createUser on system") {
    // WHEN
    val result = execute( "EXPLAIN CALL dbms.security.createUser('Alice', 'foo')" )

    //THEN
    result.executionPlanDescription() should haveAsRoot.aPlan( "dbms.security.createUser" )
  }

  test("should execute dbms.security.createUser with return values on system") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('Alice', 'foo') RETURN 'yay'")

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user("Alice"))
    testUserLogin("Alice", "foo", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should not execute dbms.security.createUser with more clauses on system") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "MATCH (anything) " +
        "CALL dbms.security.createUser('Alice', 'foo') " +
        "RETURN anything")
    }
    exception.getMessage should include("Not a recognised system command or procedure. This Cypher command can only be executed in a user database:")

    val exception2 = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "MERGE (anything) WITH anything " +
        "CALL dbms.security.createUser('Alice', 'foo') " +
        "RETURN anything")
    }
    exception2.getMessage should include("Not a recognised system command or procedure. This Cypher command can only be executed in a user database:")


    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive)
  }

  test("should execute dbms.security.createUser on system with username parameter") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser($name, 'foo')", Map[String, Object]("name" -> "Alice").asJava)

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user("Alice"))
    testUserLogin("Alice", "foo", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should execute dbms.security.createUser on system with password parameter") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('Alice', $password)", Map[String, Object]("password" -> "foo").asJava)

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUserActive, user("Alice"))
    testUserLogin("Alice", "foo", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail dbms.security.createUser for existing user") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    val exception = the[QueryExecutionException] thrownBy {
      executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('neo4j', 'foo')")
    }
    exception.getMessage should include("Failed to create the specified user 'neo4j': User already exists.")
    testUserLogin("neo4j", "neo", AuthenticationResult.SUCCESS)
  }

  test("should fail using non-system procedure on system") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "CALL db.createLabel('Foo')") // any procedure that we will never allow on system should be here
    }
    exception.getMessage should include("Not a recognised system command or procedure. This Cypher command can only be executed in a user database:")
  }

  test("should fail using non-existing procedure on system") {
    // GIVEN
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "CALL dbms.something.something.profit()")
    }
    exception.getMessage should include("There is no procedure with the name `dbms.something.something.profit` registered for this database instance.")
  }

  Seq(
    ("CALL dbms.security.createUser('Alice', 'foo')", "dbms.security.createUser"),
    ("CALL dbms.security.changeUserPassword('Alice', 'foo')", "dbms.security.changeUserPassword"),
    ("CALL dbms.security.addRoleToUser('Role', 'Alice')", "dbms.security.addRoleToUser"),
    ("CALL dbms.security.removeRoleFromUser('Role', 'Alice')", "dbms.security.removeRoleFromUser"),
    ("CALL dbms.security.deleteUser('Alice')", "dbms.security.deleteUser"),
    ("CALL dbms.security.suspendUser('Alice')", "dbms.security.suspendUser"),
    ("CALL dbms.security.activateUser('Alice')", "dbms.security.activateUser"),
    ("CALL dbms.security.listUsers()", "dbms.security.listUsers"),
    ("CALL dbms.security.listRoles()", "dbms.security.listRoles"),
    ("CALL dbms.security.listRolesForUser('Alice')", "dbms.security.listRolesForUser"),
    ("CALL dbms.security.listUsersForRole('role')", "dbms.security.listUsersForRole"),
    ("CALL dbms.security.createRole('role')", "dbms.security.createRole"),
    ("CALL dbms.security.deleteRole('role')", "dbms.security.deleteRole")
  ).foreach {
    case (query, name) =>
      test(s"should fail using $name on default database") {
        // GIVEN
        execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

        // WHEN
        the[RuntimeException] thrownBy {
          executeOnDBMSDefault("neo4j", "neo", query)
        } should have message s"This is an administration command and it should be executed against the system database: $name"
      }
  }
}
