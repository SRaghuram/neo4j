/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings

class DBMSProceduresAcceptanceTest extends DDLAcceptanceTestBase {

  test("should execute dbms.createUser on system"){
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("neo4j", "neo", "CALL dbms.security.createUser('Alice', 'foo')")

    //THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUserActive, user("Alice"))
  }

}
