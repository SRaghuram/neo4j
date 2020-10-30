/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.Label

// Tests for actual behaviour of count() function for restricted users
class CountNodeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = clearPublicRole()

  private def countSetup(): Unit = {
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B), (:B)")

     selectDatabase(SYSTEM_DATABASE_NAME)
  }

  // Specific label, change in tx

  test("specific label, change in tx, user with full traverse") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(3) // commited (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node (doesn't match)
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  test("specific label, change in tx, user with traverse on matched label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(3) // commited (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node (doesn't match)
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  test("specific label, change in tx, user with traverse on other label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(2) // commited (:A:B) and one in TX, but not the commited (:A) node (not granted) and (:B) node (not matched)
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  test("specific label, change in tx, user with denied traverse on matched label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(1) // one in TX, but not the commited (:A), (:A:B) nodes (denied) and (:B) node (not matched)
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  test("specific label, change in tx, user with denied traverse on other label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(2) // commited (:A) and one in TX, but not the commited (:A:B) node (denied) and (:B) node (not matched and denied)
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  // Any labels, change in tx

  test("any label, change in tx, user with full traverse") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(4) // commited nodes and one in TX
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  test("any label, change in tx, user with traverse on one label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(3) // commited (:A) and (:A:B) node and one in TX, but not the commited (:B) node (not granted)
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  test("any label, change in tx, user with denied traverse on one label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN count(n) as count",
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get("count") should be(2) // commited (:B) node and one in TX, but not the commited (:A) and (:A:B) nodes (denied)
      }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)
  }

  // Specific label, change in query

  test("specific label, change in query, user with full traverse") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(3) // commited (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node (doesn't match)
      }) should be(1)
  }

  test("specific label, change in query, user with traverse on matched label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(3) // commited (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node (doesn't match)
      }) should be(1)
  }

  test("specific label, change in query, user with traverse on other label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(2) // commited (:A:B) and one in TX, but not the commited (:A) node (not granted) and (:B) node (not matched)
      }) should be(1)
  }

  test("specific label, change in query, user with denied traverse on matched label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(1) // one in TX, but not the commited (:A), (:A:B) nodes (denied) and (:B) node (not matched)
      }) should be(1)
  }

  test("specific label, change in query, user with denied traverse on other label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(2) // commited (:A) and one in TX, but not the commited (:A:B) node (denied) and (:B) node (not matched and denied)
      }) should be(1)
  }

  // Any labels, change in query

  test("any label, change in query, user with full traverse") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(4) // commited nodes and one in TX
      }) should be(1)
  }

  test("any label, change in query, user with traverse on one label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(3) // commited (:A) and (:A:B) node and one in TX, but not the commited (:B) node (not granted)
      }) should be(1)
  }

  test("any label, change in query, user with denied traverse on one label") {
    // GIVEN
    countSetup()

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (x:A) WITH x MATCH (n) RETURN count(n) as count",
      resultHandler = (row, _) => {
        row.get("count") should be(2) // commited (:B) node and one in TX, but not the commited (:A) and (:A:B) nodes (denied)
      }) should be(1)
  }
}
