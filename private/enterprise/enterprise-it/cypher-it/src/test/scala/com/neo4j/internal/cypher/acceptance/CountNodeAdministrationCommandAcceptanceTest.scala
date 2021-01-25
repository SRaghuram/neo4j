/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction

// Tests for actual behaviour of count() function on nodes for restricted users
class CountNodeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override def beforeEach(): Unit = {
    super.beforeEach()
    setupUserWithCustomRole()
    execute(s"GRANT WRITE ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B), (:B)")

     selectDatabase(SYSTEM_DATABASE_NAME)
  }

  private val createNodeFunction: InternalTransaction => Unit = tx => tx.createNode(Label.label("A"))

  private val returnCountVar = "count"
  private val matchLabelQuery = s"MATCH (n:A) RETURN count(n) as $returnCountVar"
  private val matchAllQuery = s"MATCH (n) RETURN count(n) as $returnCountVar"
  private val matchLabelQueryWithUpdate = s"CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as $returnCountVar"
  private val matchAllQueryWithUpdate = s"CREATE (x:A) WITH x MATCH (n) RETURN count(n) as $returnCountVar"

  // Specific label, change in tx

  test("specific label, change in tx, user with full traverse") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(3) // committed (:A) and (:A:B) nodes and one in TX, but not the committed (:B) node (not matched)
      }, executeBefore = createNodeFunction) should be(1)
  }

  test("specific label, change in tx, user with traverse on matched label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(3) // committed (:A) and (:A:B) nodes and one in TX, but not the committed (:B) node (not matched)
      }, executeBefore = createNodeFunction) should be(1)
  }

  test("specific label, change in tx, user with traverse on other label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES B TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(2) // committed (:A:B) and one in TX, but not the committed (:A) node (not granted) and (:B) node (not matched)
      }, executeBefore = createNodeFunction) should be(1)
  }

  test("specific label, change in tx, user with denied traverse on matched label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(1) // one in TX, but not the committed (:A), (:A:B) nodes (denied) and (:B) node (not matched)
      }, executeBefore = createNodeFunction) should be(1)
  }

  test("specific label, change in tx, user with denied traverse on other label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES B TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(2) // committed (:A) and one in TX, but not the committed (:A:B) node (denied) and (:B) node (not matched and denied)
      }, executeBefore = createNodeFunction) should be(1)
  }

  // Any labels, change in tx

  test("any label, change in tx, user with full traverse") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchAllQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(4) // committed nodes and one in TX
      }, executeBefore = createNodeFunction) should be(1)
  }

  test("any label, change in tx, user with traverse on one label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchAllQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(3) // committed (:A) and (:A:B) node and one in TX, but not the committed (:B) node (not granted)
      }, executeBefore = createNodeFunction) should be(1)
  }

  test("any label, change in tx, user with denied traverse on one label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchAllQuery,
      requiredOperator = Some("NodeCountFromCountStore"), resultHandler = (row, _) => {
        row.get(returnCountVar) should be(2) // committed (:B) node and one in TX, but not the committed (:A) and (:A:B) nodes (denied)
      }, executeBefore = createNodeFunction) should be(1)
  }

  // Specific label, change in query

  test("specific label, change in query, user with full traverse") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(3) // committed (:A) and (:A:B) nodes and one in TX, but not the committed (:B) node (not matched)
      }) should be(1)
  }

  test("specific label, change in query, user with traverse on matched label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(3) // committed (:A) and (:A:B) nodes and one in TX, but not the committed (:B) node (not matched)
      }) should be(1)
  }

  test("specific label, change in query, user with traverse on other label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES B TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(2) // committed (:A:B) and one in TX, but not the committed (:A) node (not granted) and (:B) node (not matched)
      }) should be(1)
  }

  test("specific label, change in query, user with denied traverse on matched label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(1) // one in TX, but not the committed (:A), (:A:B) nodes (denied) and (:B) node (not matched)
      }) should be(1)
  }

  test("specific label, change in query, user with denied traverse on other label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES B TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchLabelQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(2) // committed (:A) and one in TX, but not the committed (:A:B) node (denied) and (:B) node (not matched and denied)
      }) should be(1)
  }

  // Any labels, change in query

  test("any label, change in query, user with full traverse") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchAllQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(4) // committed nodes and one in TX
      }) should be(1)
  }

  test("any label, change in query, user with traverse on one label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchAllQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(3) // committed (:A) and (:A:B) node and one in TX, but not the committed (:B) node (not granted)
      }) should be(1)
  }

  test("any label, change in query, user with denied traverse on one label") {
    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, matchAllQueryWithUpdate,
      resultHandler = (row, _) => {
        row.get(returnCountVar) should be(2) // committed (:B) node and one in TX, but not the committed (:A) and (:A:B) nodes (denied)
      }) should be(1)
  }
}
