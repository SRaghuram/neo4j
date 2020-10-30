/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.security.AuthorizationViolationException

// Tests for actual behaviour of authorization rules for restricted users based on node privileges
class NodePrivilegeEnforcementAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit =  clearPublicRole()

  test("should match nodes when granted traversal privilege to custom role for all graphs and all labels") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (n:A {name:'a'})"))
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ACCESS ON DATABASE * TO custom")

    executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)") should be(0)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)", resultHandler = (row, _) => {
      row.get("labels(n)").asInstanceOf[util.Collection[String]] should contain("A")
    }) should be(1)
  }

  test("should read properties when granted read privilege to custom role for all graphs and all labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (n:A {name:'a'})"))
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {name} ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should read properties when granted MATCH privilege to custom role for all graphs and all labels") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (n:A {name:'a'})"))

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ACCESS ON DATABASE * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT MATCH {name} ON GRAPH * NODES A (*) FROM custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("should read properties when granted MATCH privilege to custom role for a specific graph") {
    // GIVEN
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")
    selectDatabase(SYSTEM_DATABASE_NAME)

    setupUserWithCustomRole(access = false)

    // WHEN
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Database access is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("read privilege for node should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {name} ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("read privilege for node should not imply traverse privilege on default graph") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {name} ON DEFAULT GRAPH NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("read privilege for relationship should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {name} ON GRAPH * RELATIONSHIPS REL (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]-() RETURN r.name") should be(0)
  }

  test("traverse privilege on default graph") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON DEFAULT GRAPH RELATIONSHIPS REL TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]-() RETURN r.name") should be(0)
  }

  test("should see properties and nodes depending on granted traverse and read privileges for role") {
    // GIVEN
    setupMultiLabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT ACCESS ON DATABASE * TO role1")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role1")
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO role1")

    execute("GRANT ACCESS ON DATABASE * TO role2")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT READ {foo} ON GRAPH * NODES A (*) TO role2")
    execute("GRANT READ {bar} ON GRAPH * NODES B (*) TO role2")

    execute("GRANT ACCESS ON DATABASE * TO role3")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ {foo} ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ {bar} ON GRAPH * NODES B (*) TO role3")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(2)
  }

  test("should see properties and nodes on default graph depending on granted traverse and read privileges for role") {
    // GIVEN
    setupMultiLabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT ACCESS ON DATABASE * TO role1")
    execute("GRANT TRAVERSE ON DEFAULT GRAPH NODES * (*) TO role1")
    execute("GRANT READ {*} ON DEFAULT GRAPH NODES * (*) TO role1")

    execute("GRANT ACCESS ON DATABASE * TO role2")
    execute("GRANT TRAVERSE ON DEFAULT GRAPH NODES * (*) TO role2")
    execute("GRANT READ {foo} ON DEFAULT GRAPH NODES A (*) TO role2")
    execute("GRANT READ {bar} ON DEFAULT GRAPH NODES B (*) TO role2")

    execute("GRANT ACCESS ON DATABASE * TO role3")
    execute("GRANT TRAVERSE ON DEFAULT GRAPH NODES A (*) TO role3")
    execute("GRANT READ {foo} ON DEFAULT GRAPH NODES A (*) TO role3")
    execute("GRANT READ {bar} ON DEFAULT GRAPH NODES B (*) TO role3")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(2)
  }

  test("should see properties and nodes depending on granted MATCH privileges for role") {
    // GIVEN
    setupMultiLabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT ACCESS ON DATABASE * TO role1")
    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO role1")

    execute("GRANT ACCESS ON DATABASE * TO role2")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT MATCH {foo} ON GRAPH * NODES A (*) TO role2")
    execute("GRANT MATCH {bar} ON GRAPH * NODES B (*) TO role2")

    execute("GRANT ACCESS ON DATABASE * TO role3")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ {foo} ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ {bar} ON GRAPH * NODES B (*) TO role3")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(2)
  }

  test("should see properties and nodes when revoking privileges for role") {
    // GIVEN
    setupMultiLabelData
    setupUserWithCustomRole(access = false)

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ACCESS ON DATABASE * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ {foo} ON GRAPH * NODES A (*) TO custom")
    execute("GRANT READ {bar} ON GRAPH * NODES B (*) TO custom")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT READ {*} ON GRAPH * NODES * (*) FROM custom")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(2)
  }

  test("should not be able to traverse labels when denied all label traversal") {
    // GIVEN
    setupUserWithCustomRole()
    setupMultiLabelData2

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {id} ON GRAPH * TO custom")

    val query = "MATCH (n) RETURN n.id, reduce(s = '', x IN labels(n) | s + ':' + x) AS labels ORDER BY n.id"

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // THEN
    val expected = List((0, ":A"),(3, ":A:B"), (4, ":A:C"), (6, ":A:B:C"))  // Nodes with label :A

    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.get("n.id"), row.get("labels")) should be(expected(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)
  }

  test("should not be able to traverse labels with grant and deny on all label traversal") {
    // GIVEN
    setupUserWithCustomRole()
    setupMultiLabelData2

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {id} ON GRAPH * TO custom")

    val query = "MATCH (n) RETURN n.id, reduce(s = '', x IN labels(n) | s + ':' + x) AS labels ORDER BY n.id"

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    val expected = // All nodes
      List(
        (0, ":A"),
        (1, ":B"),
        (2, ":C"),
        (3, ":A:B"),
        (4, ":A:C"),
        (5, ":B:C"),
        (6, ":A:B:C"),
        (7, "")
      )

    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.get("n.id"), row.get("labels")) should be(expected(index))
      }) should be(8)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)
  }

  test("should see correct nodes and labels with grant traversal on all labels and deny on specific label") {
    // GIVEN
    setupUserWithCustomRole()
    setupMultiLabelData2

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {id} ON GRAPH * TO custom")

    val query = "MATCH (n) RETURN n.id, reduce(s = '', x IN labels(n) | s + ':' + x) AS labels ORDER BY n.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    val expected1 = // All nodes
      List(
        (0, ":A"),
        (1, ":B"),
        (2, ":C"),
        (3, ":A:B"),
        (4, ":A:C"),
        (5, ":B:C"),
        (6, ":A:B:C"),
        (7, "")
      )

    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.get("n.id"), row.get("labels")) should be(expected1(index))
      }) should be(8)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")

    // THEN

    val expected2 = // All nodes without label :B
      List(
        (0, ":A"),
        (2, ":C"),
        (4, ":A:C"),
        (7, "")
      )

    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.get("n.id"), row.get("labels")) should be(expected2(index))
      }) should be(4)
  }

  test("should be able to see multi-label nodes if one label is whitelisted and none blacklisted") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B), (:A), (:B)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN n") should be(2)
    executeOnDefault("joe", "soap", "MATCH (n:B) RETURN n") should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN n") should be(1)
    executeOnDefault("joe", "soap", "MATCH (n:B) RETURN n") should be(0)

  }

  test("should see correct nodes and labels with grant and deny traversal on specific labels") {
    // GIVEN
    setupUserWithCustomRole()
    setupMultiLabelData2

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {id} ON GRAPH * TO custom")

    val query = "MATCH (n) RETURN n.id, reduce(s = '', x IN labels(n) | s + ':' + x) AS labels ORDER BY n.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")

    // THEN
    val expected = List((0, ":A"),(4, ":A:C"))  // Nodes with label :A but not :B

    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.get("n.id"), row.get("labels")) should be(expected(index))
      }) should be(2)
  }

  test("should only see properties using properties() function when having read privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should equal(util.Collections.emptyMap())
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo} ON GRAPH * NODES A TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("foo", 1L), // :A
      util.Map.of("foo", 5L), // :A:B
      util.Collections.emptyMap(), //:B or no labels
      util.Collections.emptyMap() //:B or no labels
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected1(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {bar} ON GRAPH * NODES * TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("bar", 4L), //:B
      util.Map.of("bar", 8L) //no labels
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected2(index))
    }) should be(4)

  }

  test("should not be able read properties when denied read privilege for all labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, _) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be((null, null))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, _) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be((null, null))
      }) should be(4)
  }

  test("should not be able read properties using properties() function when denied read privilege for all labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should equal(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, _) => {
        row.get("props") should equal(util.Collections.emptyMap())
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, _) => {
        row.get("props") should equal(util.Collections.emptyMap())
      }) should be(4)
  }

  test("should read correct properties when denied read privilege for all labels and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * NODES * (*) TO custom")

    val expected2 = List(
      (null, 2), // :A
      (null, 4), // :B
      (null, 6), // :A:B
      (null, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties using properties() function when denied read privilege for all labels and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * NODES * (*) TO custom")

    val expected2 = List(
      util.Map.of("bar", 2L), // :A
      util.Map.of("bar", 4L), // :B
      util.Map.of("bar", 6L), // :A:B
      util.Map.of("bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties when denied read privilege for specific labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      (3, 4), // :B
      (7, 8), // no labels
      (null, null), // :A or :A:B
      (null, null) // :A or :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties using properties() function when denied read privilege for specific labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 7L, "bar", 8L), // no labels
      util.Collections.emptyMap(), // :A or :A:B
      util.Collections.emptyMap() // :A or :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties when denied read privilege for specific label and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      (3, 4), // :B
      (7, 8), // no labels
      (null, 2), // :A
      (null, 6) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties using properties() function when denied read privilege for specific label and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 7L, "bar", 8L), // no labels
      util.Map.of("bar", 2L), // :A
      util.Map.of("bar", 6L) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties with several grants and denies on read labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, null), // :A
      (3, null), // :B
      (5, null), // :A:B
      (7, null) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      (3, null), // :B
      (7, null), // no labels
      (null, null), // :A or :A:B
      (null, null) // :A or :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {bar} ON GRAPH * NODES A (*) TO custom")

    val expected3 = List(
      (3, null), // :B
      (7, null), // no labels
      (null, 2), // :A
      (null, 6) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {bar} ON GRAPH * NODES B (*) TO custom")

    val expected4 = List(
      (3, null), // :B
      (7, null), // no labels
      (null, 2), // :A
      (null, null) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected4(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    val expected5 = List(
      (3, null), // :B
      (7, 8), // no labels
      (null, 2), // :A
      (null, null) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected5(index))
      }) should be(4)
  }

  test("should read correct properties using properties() function with several grants and denies on read labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L), // :A
      util.Map.of("foo", 3L), // :B
      util.Map.of("foo", 5L), // :A:B
      util.Map.of("foo", 7L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      util.Map.of("foo", 3L), // :B
      util.Map.of("foo", 7L), // no labels
      util.Collections.emptyMap(), // :A or :A:B
      util.Collections.emptyMap() // :A or :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {bar} ON GRAPH * NODES A (*) TO custom")

    val expected3 = List(
      util.Map.of("foo", 3L), // :B
      util.Map.of("foo", 7L), // no labels
      util.Map.of("bar", 2L), // :A
      util.Map.of("bar", 6L) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected3(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {bar} ON GRAPH * NODES B (*) TO custom")

    val expected4 = List(
      util.Map.of("foo", 3L), // :B
      util.Map.of("foo", 7L), // no labels
      util.Map.of("bar", 2L), // :A
      util.Collections.emptyMap() // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected4(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")

    val expected5 = List(
      util.Map.of("foo", 3L), // :B
      util.Map.of("foo", 7L, "bar", 8L), // no labels
      util.Map.of("bar", 2L), // :A
      util.Collections.emptyMap() // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected5(index))
      }) should be(4)
  }

  test("should not be able read properties when denied match privilege for all labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)
  }

  test("should not be able read properties using properties() function when denied match privilege for all labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)
  }

  test("should read correct properties when denied match privilege for all labels and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {foo} ON GRAPH * NODES * (*) TO custom")

    val expected2 = List(
      (null, 2), // :A
      (null, 4), // :B
      (null, 6), // :A:B
      (null, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties using properties() function when denied match privilege for all labels and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n)as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {foo} ON GRAPH * NODES * (*) TO custom")

    val expected2 = List(
      util.Map.of("bar", 2L), // :A
      util.Map.of("bar", 4L), // :B
      util.Map.of("bar", 6L), // :A:B
      util.Map.of("bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties when denied match privilege for specific labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {*} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      (3, 4), // :B
      (7, 8), // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(2)
  }

  test("should read correct properties using properties() function when denied match privilege for specific labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {*} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 7L, "bar", 8L), // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(2)
  }

  test("should read correct properties when denied match privilege for specific label and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      (1, 2), // :A
      (3, 4), // :B
      (5, 6), // :A:B
      (7, 8) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {foo} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      (3, 4), // :B
      (7, 8), // no labels
      (null, 2), // :A
      (null, 6) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties using properties function() when denied match privilege for specific label and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultiLabelData

    val query = "MATCH (n) RETURN properties(n) as props ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * (*) TO custom")

    val expected1 = List(
      util.Map.of("foo", 1L, "bar", 2L), // :A
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 5L, "bar", 6L), // :A:B
      util.Map.of("foo", 7L, "bar", 8L) // no labels
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected1(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {foo} ON GRAPH * NODES A (*) TO custom")

    val expected2 = List(
      util.Map.of("foo", 3L, "bar", 4L), // :B
      util.Map.of("foo", 7L, "bar", 8L), // no labels
      util.Map.of("bar", 2L), // :A
      util.Map.of("bar", 6L) // :A:B
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(4)
  }

  test("should get correct nodes from NodeByIdSeek") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT READ {*} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    for ( _ <- 0 until 100 ) {
      createLabeledNode(Map("prop" -> "visible"), "A")
      createLabeledNode(Map("prop" -> "visible"), "A", "B")
      createLabeledNode(Map("prop" -> "secret"), "B")
    }

    // WHEN
    val query = "MATCH (n) WHERE id(n) IN [0, 1, 2, 1337] RETURN n.prop ORDER BY n.prop"

    // THEN

    // Restricted user
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, _) => {
        row.get("n.prop") should be("visible")
      }, requiredOperator = Some("NodeByIdSeek")) should be(2)

    // Unrestricted user
    val result = execute(query)
    result.toList should be(List(Map("n.prop" -> "secret"), Map("n.prop" -> "visible"), Map("n.prop" -> "visible")))
    mustHaveOperator(result.executionPlanDescription(), "NodeByIdSeek")
  }

  // Index tests

  test("should get the correct values from index") {
    //GIVEN
    setupUserWithCustomRole("customUser", "secret", "customRole")
    execute("GRANT TRAVERSE ON GRAPH * TO customRole")
    execute("GRANT READ {foo} ON GRAPH * NODES A TO customRole")
    execute("DENY READ {foo} ON GRAPH * NODES C TO customRole")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {foo: 1}), (:B {foo: 2}), (:C {foo: 3}), (:A:B {foo: 4}), (:A:C {foo: 5})")

    val queryMatch = "MATCH (n) RETURN n.foo ORDER BY n.foo"
    val queryEquals = "MATCH (n:A) WHERE n.foo = 5 RETURN n.foo ORDER BY n.foo"
    val queryRange = "MATCH (n:A) WHERE n.foo < 8 RETURN n.foo ORDER BY n.foo"
    val queryExists = "MATCH (n:A) WHERE exists(n.foo) RETURN n.foo ORDER BY n.foo"

    val expectedMatch = List(1, 4, null, null, null)
    val expectedRangeExists = List(1, 4)
    val expectedEquals = (_: Int) => fail("should get no rows")

    // without index
    // WHEN .. THEN
    Seq(
      (queryMatch, expectedMatch, 5),
      (queryEquals, expectedEquals, 0),
      (queryRange, expectedRangeExists, 2),
      (queryExists, expectedRangeExists, 2)
    ).foreach {
      case (query, expected, nbrRows) =>
        executeOnDefault("customUser", "secret", query, resultHandler = (row, index) => {
          row.getNumber("n.foo") should be(expected(index))
        }) should be(nbrRows)
    }

    // with index
    graph.createIndex("A", "foo")

    // WHEN .. THEN
    Seq(
      (queryMatch, expectedMatch, 5),
      (queryEquals, expectedEquals, 0),
      (queryRange, expectedRangeExists, 2),
      (queryExists, expectedRangeExists, 2)
    ).foreach {
      case (query, expected, nbrRows) =>
        executeOnDefault("customUser", "secret", query, resultHandler = (row, index) => {
          row.getNumber("n.foo") should be(expected(index))
        }) should be(nbrRows)
    }
  }

  test("should get the correct values from composite index") {
    //GIVEN
    setupUserWithCustomRole("customUser", "secret", "customRole")
    execute("GRANT TRAVERSE ON GRAPH * TO customRole")
    execute("GRANT READ {foo} ON GRAPH * NODES A TO customRole")
    execute("GRANT READ {prop} ON GRAPH * NODES B TO customRole")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {foo: 1, prop: 6}), (:B {foo: 2, prop: 7}), (:C {foo: 3, prop: 8}), (:A:B {foo: 4, prop: 9}), (:A:C {foo: 5, prop: 10})")

    val queryExists = "MATCH (n:A) WHERE exists(n.foo) AND exists(n.prop) RETURN n.foo, n.prop"
    val queryEquals = "MATCH (n:A) WHERE n.foo = 5 AND n.prop = 10 RETURN n.foo, n.prop"
    val queryRange = "MATCH (n:A) WHERE n.foo < 5 AND exists(n.prop) RETURN n.foo, n.prop"

    val expectedRangeExists = (4, 9)
    val expectedEquals = () => fail("should get no rows")


    // without index
    // WHEN .. THEN
    Seq(
      (queryEquals, expectedEquals, 0),
      (queryRange, expectedRangeExists, 1),
      (queryExists, expectedRangeExists, 1)
    ).foreach {
      case (query, expected, nbrRows) =>
        executeOnDefault("customUser", "secret", query, resultHandler = (row, _) => {
          (row.getNumber("n.foo"), row.getNumber("n.prop")) should be(expected)
        }) should be(nbrRows)
    }

    // with index
    graph.createIndex("A", "foo", "prop")

    // WHEN .. THEN
    Seq(
      (queryEquals, expectedEquals, 0),
      (queryRange, expectedRangeExists, 1),
      (queryExists, expectedRangeExists, 1)
    ).foreach {
      case (query, expected, nbrRows) =>
        executeOnDefault("customUser", "secret", query, resultHandler = (row, _) => {
          (row.getNumber("n.foo"), row.getNumber("n.prop")) should be(expected)
        }) should be(nbrRows)
    }
  }

  test("should get the correct values from composite index with deny") {
    //GIVEN
    setupUserWithCustomRole("customUser", "secret", "customRole")
    execute("GRANT TRAVERSE ON GRAPH * TO customRole")
    execute("GRANT READ {foo, prop} ON GRAPH * NODES A TO customRole")
    execute("DENY READ {prop} ON GRAPH * NODES C TO customRole")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {foo: 1, prop: 6}), (:B {foo: 2, prop: 7}), (:C {foo: 3, prop: 8}), (:A:B {foo: 4, prop: 9}), (:A:C {foo: 5, prop: 10})")

    val queryExists = "MATCH (n:A) WHERE exists(n.foo) AND exists(n.prop) RETURN n.foo, n.prop ORDER BY n.foo"
    val queryEquals = "MATCH (n:A) WHERE n.foo = 5 AND n.prop = 10 RETURN n.foo, n.prop ORDER BY n.foo"
    val queryRange = "MATCH (n:A) WHERE n.foo < 5 AND exists(n.prop) RETURN n.foo, n.prop ORDER BY n.foo"

    val expectedRangeExists = List((1, 6), (4, 9))
    val expectedEquals = (_: Int) => fail("should get no rows")

    // without index
    // WHEN .. THEN
    Seq(
      (queryEquals, expectedEquals, 0),
      (queryRange, expectedRangeExists, 2),
      (queryExists, expectedRangeExists, 2)
    ).foreach {
      case (query, expected, nbrRows) =>
        executeOnDefault("customUser", "secret", query, resultHandler = (row, index) => {
          (row.getNumber("n.foo"), row.getNumber("n.prop")) should be(expected(index))
        }) should be(nbrRows)
    }

    // with index
    graph.createIndex("A", "foo", "prop")

    // WHEN .. THEN
    Seq(
      (queryEquals, expectedEquals, 0),
      (queryRange, expectedRangeExists, 2),
      (queryExists, expectedRangeExists, 2)
    ).foreach {
      case (query, expected, nbrRows) =>
        executeOnDefault("customUser", "secret", query, resultHandler = (row, index) => {
          (row.getNumber("n.foo"), row.getNumber("n.prop")) should be(expected(index))
        }) should be(nbrRows)
    }
  }

  test("unique index locking test") {
    setupMultiLabelData
    graph.createUniqueIndex("A", "foo")
    setupUserWithCustomRole("user", "secret", "role")
    // role whitelist A and blacklist B
    execute("GRANT READ {foo,bar} ON GRAPH * NODES * TO role")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO role")
    execute("DENY TRAVERSE ON GRAPH * NODES B TO role")
    execute("GRANT WRITE ON GRAPH * TO role")

    executeOnDefault("user", "secret", "MERGE (n:A {foo: 1})")

    val exception = the[QueryExecutionException] thrownBy {
      executeOnDefault("user", "secret", "MERGE (n:A {foo: 5})")
    }

    exception.getMessage should include("already exists with label `A` and property `foo` = 5")
  }

  test("should support whitelist and blacklist traversal in index seeks") {
    setupMultiLabelData
    graph.createIndex("A", "foo")
    setupUserWithCustomRole("user1", "secret", "role1")
    setupUserWithCustomRole("user2", "secret", "role2")
    setupUserWithCustomRole("user3", "secret", "role3")

    // role1 whitelist A
    execute("GRANT READ {foo} ON GRAPH * NODES * TO role1")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO role1")

    // role2 whitelist A and blacklist B
    execute("GRANT READ {foo} ON GRAPH * NODES * TO role2")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO role2")
    execute("DENY TRAVERSE ON GRAPH * NODES B TO role2")

    // role3 whitelist all labels and blacklist B
    execute("GRANT READ {foo} ON GRAPH * NODES * TO role3")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO role3")
    execute("DENY TRAVERSE ON GRAPH * NODES B TO role3")

    // index with equality
    Seq("user1" -> 1, "user2" -> 0, "user3" -> 0).foreach {
      case (user, count) =>
        val query = "MATCH (n:A) WHERE n.foo = 5 RETURN count(n)"
        executeOnDefault(user, "secret", query, resultHandler = (row, _) => {
          withClue(s"User '$user' should get count $count for query '$query'") {
            row.get("count(n)") should be(count)
          }
        })
    }

    // index with range
    Seq("user1" -> 2, "user2" -> 1, "user3" -> 1).foreach {
      case (user, count) =>
        val query = "MATCH (n:A) WHERE n.foo > 0 RETURN count(n)"
        executeOnDefault(user, "secret", query, resultHandler = (row, _) => {
          withClue(s"User '$user' should get count $count for query '$query'") {
            row.get("count(n)") should be(count)
          }
        })
    }

    // index with exists
    Seq("user1" -> 2, "user2" -> 1, "user3" -> 1).foreach {
      case (user, count) =>
        val query = "MATCH (n:A) WHERE exists(n.foo) RETURN count(n)"
        executeOnDefault(user, "secret", query, resultHandler = (row, _) => {
          withClue(s"User '$user' should get count $count for query '$query'") {
            row.get("count(n)") should be(count)
          }
        })
    }
  }

  // helper variable, methods and class

  private def setupMultiLabelData = {
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {foo:1, bar:2})")
    execute("CREATE (n:B {foo:3, bar:4})")
    execute("CREATE (n:A:B {foo:5, bar:6})")
    execute("CREATE (n {foo:7, bar:8})")
  }

  private def setupMultiLabelData2 = {
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A     {id:0})")
    execute("CREATE (:B     {id:1})")
    execute("CREATE (:C     {id:2})")
    execute("CREATE (:A:B   {id:3})")
    execute("CREATE (:A:C   {id:4})")
    execute("CREATE (:B:C   {id:5})")
    execute("CREATE (:A:B:C {id:6})")
    execute("CREATE (       {id:7})")
  }
}
