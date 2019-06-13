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

class DBMSProceduresAcceptanceTest extends DDLAcceptanceTestBase {

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
    exception.getMessage should include("The specified user 'neo4j' already exists.")
    testUserLogin("neo4j", "neo", AuthenticationResult.SUCCESS)
  }
}
