/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.internal.kernel.api.security.AuthenticationResult

import scala.collection.JavaConverters._

class DBMSProceduresAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should execute dbms.security.createUser on system") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('Alice', 'foo')")

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive, user("Alice"))
    testUserLogin("Alice", "foo", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should explain dbms.security.createUser on system") {
    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    val result = execute( "EXPLAIN CALL dbms.security.createUser('Alice', 'foo')" )

    //THEN
    result.executionPlanDescription() should haveAsRoot.aPlan( "dbms.security.createUser" )
  }

  test("should execute dbms.security.createUser with return values on system") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('Alice', 'foo') RETURN 'yay'")

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive, user("Alice"))
    testUserLogin("Alice", "foo", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should not execute dbms.security.createUser with more clauses on system") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "MATCH (anything) " +
        "CALL dbms.security.createUser('Alice', 'foo') " +
        "RETURN anything")
    }
    exception.getMessage should include("Not a recognised system command or procedure")

    val exception2 = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "MERGE (anything) WITH anything " +
        "CALL dbms.security.createUser('Alice', 'foo') " +
        "RETURN anything")
    }
    exception2.getMessage should include("Not a recognised system command or procedure")


    //THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive)
  }

  test("should execute dbms.security.createUser on system with parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('Alice', $password)", Map[String, Object]("password" -> "foo").asJava)

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive, user("Alice"))
    testUserLogin("Alice", "foo", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail dbms.security.createUser for existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "CALL db.createLabel('Foo')") // any procedure that we will never allow on system should be here
    }
    exception.getMessage should include("Not a recognised system command or procedure")
  }

  test("should fail using non-existing procedure on system") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    val exception = the[RuntimeException] thrownBy {
      executeOnSystem("neo4j", "neo", "CALL dbms.something.something.profit()")
    }
    exception.getMessage should include("There is no procedure with the name `dbms.something.something.profit` registered for this database instance.")
  }
}
