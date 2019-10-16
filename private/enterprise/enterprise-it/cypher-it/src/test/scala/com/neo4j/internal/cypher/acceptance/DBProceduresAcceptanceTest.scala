/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.Relationship

class DBProceduresAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  /*
    Please note: Since we have other (better) tests, that tests the correctness of these procedures,
                 the tests in this class will not look at the return values and check on that but just the number of returned rows
   */

  /*
    ------------ db.relationshipTypes ------------
  */

  test("db.relationshipTypes should return empty result for user without any whitelisted type") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    setupUserWithCustomRole()

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return empty result for user with only read but not traverse") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    setupUserWithCustomRole()
    execute("GRANT READ {prop} ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return empty result for user with only denied read") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    setupUserWithCustomRole()
    execute("DENY READ {prop} ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return empty result for user with only write") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    setupUserWithCustomRole()
    execute("GRANT WRITE {*} ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return empty result for user with denied write") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    setupUserWithCustomRole()
    execute("DENY WRITE {*} ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return correct result for user with whitelisted type") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 1
  }

  test("db.relationshipTypes should return empty result for user with denied traverse") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    setupUserWithCustomRole()
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return correct results for user with whitelisted type") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('B')")
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("neo4j", "neo", "CALL db.relationshipTypes()") shouldBe 2

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 1
  }

  test("db.relationshipTypes should return correct results for user with all whitelisted types and denied ones") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('A')")
    executeOnDefault("neo4j", "neo", "CALL db.createRelationshipType('B')")
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("neo4j", "neo", "CALL db.relationshipTypes()") shouldBe 2

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 1
  }

  /*
    ------------ db.labels ------------
  */

  test("db.labels should return empty result for user without any whitelisted label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    setupUserWithCustomRole()

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return empty result for user with only read but not traverse") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    setupUserWithCustomRole()
    execute("GRANT READ {prop} ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return empty result for user with only denied read") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    setupUserWithCustomRole()
    execute("DENY READ {prop} ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return empty result for user with only write") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    setupUserWithCustomRole()
    execute("GRANT WRITE {*} ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return empty result for user with only denied write") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    setupUserWithCustomRole()
    execute("DENY WRITE {*} ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return correct result for user with whitelisted label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 1
  }

  test("db.labels should return empty result for user with denied traverse") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    setupUserWithCustomRole()
    execute("DENY TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }


  test("db.labels should return correct results for user with whitelisted label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('B')")
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("neo4j", "neo", "CALL db.labels()") shouldBe 2

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 1
  }

  test("db.labels should return correct results for userwith all whitelisted labels and denied ones") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('A')")
    executeOnDefault("neo4j", "neo", "CALL db.createLabel('B')")
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("neo4j", "neo", "CALL db.labels()") shouldBe 2

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.labels()") shouldBe 1
  }

  /*
    ------------ db.propertyKeys ------------
  */

  test("db.propertyKeys should return empty result for user without any grants") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE ({a:1})")
    setupUserWithCustomRole()

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with only write") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE ({a:1})")
    setupUserWithCustomRole()
    execute("GRANT WRITE {*} ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with only denied write") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE ({a:1})")
    setupUserWithCustomRole()
    execute("DENY WRITE {*} ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with only traverse grant") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A{a:1})")
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with denied traverse") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A{a:1})")
    setupUserWithCustomRole()
    execute("DENY TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return correct result for user with only read grant") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A{a:1})")
    setupUserWithCustomRole()
    execute("GRANT READ{a} ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1
  }

  test("db.propertyKeys should return empty result for user with denied read") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A{a:1})")
    setupUserWithCustomRole()
    execute("DENY READ{a} ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return correct result for user with match on any label as long as that propertyKey is part of the grant") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A{x:1})")
    setupUserWithCustomRole()
    execute("GRANT MATCH {x} ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    // GIVEN
    executeOnSystem("neo4j", "neo","REVOKE GRANT READ {x} ON GRAPH * NODES A (*) FROM custom")
    executeOnSystem("neo4j", "neo","REVOKE GRANT TRAVERSE ON GRAPH * NODES A (*) FROM custom")

    // GRANT on Label B, but same propKey should still work
    executeOnSystem("neo4j", "neo","GRANT MATCH {x} ON GRAPH * NODES B (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    // GIVEN
    executeOnSystem("neo4j", "neo","REVOKE GRANT READ {x} ON GRAPH * NODES B (*) FROM custom")
    executeOnSystem("neo4j", "neo","REVOKE GRANT TRAVERSE ON GRAPH * NODES B (*) FROM custom")

    // GRANT on all Labels, but same propKey should still work
    executeOnSystem("neo4j", "neo","GRANT MATCH {x} ON GRAPH * NODES * (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1
  }

  test("db.propertyKeys should return correct result for user with match on any label but deny on one") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A{x:1})")
    setupUserWithCustomRole()
    execute("GRANT MATCH {x, y} ON GRAPH * NODES * (*) TO custom")
    execute("DENY MATCH {x} ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 2

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {x} ON GRAPH * NODES * (*) TO custom")
    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    // GIVEN
    executeOnSystem("neo4j", "neo","REVOKE GRANT READ {x} ON GRAPH * NODES * (*) FROM custom")
    executeOnSystem("neo4j", "neo","REVOKE GRANT TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1
  }

  test("db.propertyKeys should return correct result for user with match on any label and type as long as that propertyKey is part of the grant") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A{a:1, c:3})-[:REL{b:2, c:3}]->()")
    setupUserWithCustomRole()
    execute("GRANT MATCH {b} ON GRAPH * RELATIONSHIPS REL (*) TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    executeOnSystem("neo4j", "neo","GRANT MATCH {a} ON GRAPH * NODES A (*) TO custom")
    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 2

    executeOnSystem("neo4j", "neo","GRANT READ {c} ON GRAPH * ELEMENTS * (*) TO custom")
    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 3

    executeOnSystem("neo4j", "neo","DENY READ {c} ON GRAPH * ELEMENTS * (*) TO custom")
    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 2

    executeOnSystem("neo4j", "neo","DENY READ {*} ON GRAPH * ELEMENTS * (*) TO custom")
     // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  /*
    ------------ OTHERS ------------
   */

  test("make sure that db.schema.nodeTypeProperties does not leak for user without grants") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE ({a:1})")
    setupUserWithCustomRole()

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.schema.nodeTypeProperties()") shouldBe 0

    // WHEN & THEN
    executeOnDefault("neo4j", "neo", "CALL db.schema.nodeTypeProperties()") shouldBe 1
  }

  test("make sure that db.schema.relTypeProperties does not leak for user without grants") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE ()-[:REL{a:1}]->()")
    setupUserWithCustomRole()

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.schema.relTypeProperties()") shouldBe 0

    // WHEN & THEN
    executeOnDefault("neo4j", "neo", "CALL db.schema.relTypeProperties()") shouldBe 1
  }

  test("make sure that db.schema.visualization does not leak for user without grants") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("ALTER USER neo4j SET PASSWORD 'neo' CHANGE NOT REQUIRED")
    executeOnDefault("neo4j", "neo", "CREATE (:A)-[:REL]->(:B)")
    setupUserWithCustomRole()

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.schema.visualization()", resultHandler = (row, _) => {
      row.get("relationships") should equal(util.Collections.EMPTY_LIST)
      row.get("nodes") should equal(util.Collections.EMPTY_LIST)
    }) shouldBe 1

    // WHEN & THEN
    executeOnDefault("neo4j", "neo", "CALL db.schema.visualization()", resultHandler = (row, _) => {
      row.get("relationships").asInstanceOf[util.ArrayList[Relationship]].size() shouldBe 1
      row.get("nodes").asInstanceOf[util.ArrayList[Relationship]].size() shouldBe 2
    }) shouldBe 1
  }
}
