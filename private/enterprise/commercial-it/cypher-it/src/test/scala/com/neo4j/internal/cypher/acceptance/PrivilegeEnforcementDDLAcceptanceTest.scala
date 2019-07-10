/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.{Node, Result}
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext

// Tests for actual behaviour of authorization rules for restricted users based on privileges
class PrivilegeEnforcementDDLAcceptanceTest extends DDLAcceptanceTestBase {

  // Tests for node privileges

  test("should match nodes when granted traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)", resultHandler = (row, _) => {
      row.get("labels(n)").asInstanceOf[util.Collection[String]] should contain("A")
    }) should be(1)
  }

  test("should read properties when granted read privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should read properties when granted MATCH privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (name) ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ (name) ON GRAPH * NODES A (*) FROM custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)
  }

  test("should read properties when granted MATCH privilege to custom role for a specific database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")
    selectDatabase(SYSTEM_DATABASE_NAME)

    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT MATCH (*) ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Read operations are not allowed for user 'joe' with roles [custom]."
  }

  test("read privilege for node should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("read privilege for relationship should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * RELATIONSHIPS REL (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]-() RETURN r.name") should be(0)
  }

  test("read privilege for element should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name: 'n1'})-[:A {name:'r'}]->(:A {name: 'n2'})")
    val query = "MATCH (n1)-[r]->(n2) RETURN n1.name, r.name, n2.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * ELEMENTS A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * ELEMENTS A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(("n1", "r", "n2"))
    }) should be(1)
  }

  test("should see correct things when granted element privileges") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name: 'a1'})-[:A {name: 'ra1'}]->(:A {name: 'a2'})")
    execute("CREATE (:A {name: 'a3'})-[:B {name: 'rb1'}]->(:A {name: 'a4'})")
    execute("CREATE (:B {name: 'b1'})-[:A {name: 'ra2'}]->(:B {name: 'b2'})")
    execute("CREATE (:B {name: 'b3'})-[:B {name: 'rb2'}]->(:B {name: 'b4'})")
    val query = "MATCH (n1)-[r]->(n2) RETURN n1.name, r.name, n2.name ORDER BY n1.name, r.name"

    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", query)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * ELEMENTS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be((null, null, null))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * ELEMENTS A, B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(("a1", "ra1", "a2"))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (name) ON GRAPH * ELEMENTS B TO custom")

    // THEN
    val expected1 = Seq(
      ("a1", "ra1", "a2"),
      ("a3", "rb1", "a4"),
      ("b1", "ra2", "b2"),
      ("b3", "rb2", "b4")
    )
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(expected1(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ (name) ON GRAPH * ELEMENTS A FROM custom")

    // THEN
    val expected2 = Seq(
      ("b1", null, "b2"),
      ("b3", "rb2", "b4"),
      (null, "rb1", null),
      (null, null, null)
    )
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(expected2(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE MATCH (name) ON GRAPH * ELEMENTS B FROM custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be((null, null, null))
    }) should be(4) // TODO: should be 1 when revoking MATCH also revokes traverse

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * ELEMENTS C TO custom") // unrelated privilege, just so we don't remove all access
    execute("REVOKE TRAVERSE ON GRAPH * ELEMENTS A, B FROM custom") // TODO: won't work when revoking MATCH also revokes traverse, need to re-add traverse B

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)
  }

  test("should see properties and nodes depending on granted traverse and read privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role1")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role2")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role2")

    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role3")

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
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT MATCH (foo) ON GRAPH * NODES A (*) TO role2")
    execute("GRANT MATCH (bar) ON GRAPH * NODES B (*) TO role2")

    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role3")

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
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO custom")

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
    execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom")

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
    execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")

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
    execute("GRANT READ (id) ON GRAPH * TO custom")

    val query = "MATCH (n) RETURN n.id, reduce(s = '', x IN labels(n) | s + ':' + x) AS labels ORDER BY n.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)
  }

  test("should not be able to traverse labels with grant and deny on all label traversal") {
    // GIVEN
    setupUserWithCustomRole()
    setupMultiLabelData2

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (id) ON GRAPH * TO custom")

    val query = "MATCH (n) RETURN n.id, reduce(s = '', x IN labels(n) | s + ':' + x) AS labels ORDER BY n.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)
  }

  test("should see correct nodes and labels with grant traversal on all labels and deny on specific label") {
    // GIVEN
    setupUserWithCustomRole()
    setupMultiLabelData2

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (id) ON GRAPH * TO custom")

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

  test("should see correct nodes and labels with grant and deny traversal on specific labels") {
    // GIVEN
    setupUserWithCustomRole()
    setupMultiLabelData2

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (id) ON GRAPH * TO custom")

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

  test("should get correct labels from procedure") {
    // GIVEN
    setupUserWithCustomRole()

    // Currently you need to have some kind of traverse or read access to be able to call the procedure at all
    execute("GRANT TRAVERSE ON GRAPH * NODES ignore TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B:E), (:B:C), (:C:D)")

    val query = "CALL db.labels() YIELD label, nodeCount as count RETURN label, count ORDER BY label"

    // WHEN..THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("result should be empty")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    val expected = List(("A", 2), ("B", 1), ("E", 1))
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("label"), row.get("count")) should be(expected(index))
    }) should be(3)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("label"), row.get("count")) should be(("A", 1))
    }) should be(1)
  }

  test("should not be able read properties when denied read privilege for all labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY READ (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, _) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be((null, null))
      }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, _) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be((null, null))
      }) should be(4)
  }

  test("should read correct properties when denied read privilege for all labels and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY READ (foo) ON GRAPH * NODES * (*) TO custom")

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
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties when denied read privilege for specific labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY READ (*) ON GRAPH * NODES A (*) TO custom")

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
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties when denied read privilege for specific label and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY READ (foo) ON GRAPH * NODES A (*) TO custom")

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
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)
  }

  test("should read correct properties with several grants and denies on read labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY READ (foo) ON GRAPH * NODES A (*) TO custom")

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
    execute("GRANT READ (bar) ON GRAPH * NODES A (*) TO custom")

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
    execute("DENY READ (bar) ON GRAPH * NODES B (*) TO custom")

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
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

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

  test("should not be able read properties when denied match privilege for all labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY MATCH (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)
  }

  test("should read correct properties when denied match privilege for all labels and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY MATCH (foo) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)
  }

  test("should read correct properties when denied match privilege for specific labels and all properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY MATCH (*) ON GRAPH * NODES A (*) TO custom")

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
    execute("GRANT MATCH (*) ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(2)
  }

  test("should read correct properties when denied match privilege for specific label and specific property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    setupMultilabelData

    val query = "MATCH (n) RETURN n.foo, n.bar ORDER BY n.foo, n.bar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo,bar) ON GRAPH * NODES * (*) TO custom")

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
    execute("DENY MATCH (foo) ON GRAPH * NODES A (*) TO custom")

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
    execute("GRANT MATCH (foo) ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(2)
  }

  // Tests for relationship privileges

  test("should find relationship when granted traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})")

    val expected = List(
      "a", "b"
    )
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, index) => {
      row.get("n.name") should be(expected(index))
    }) should be(2)

    executeOnDefault("joe", "soap", "MATCH (n)-->(m) RETURN n.name", resultHandler = (_, _) => {
      fail("should not get a match")
    }) should be(0)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    executeOnDefault("joe", "soap", "MATCH (n)-->(m) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should not find relationship when denied all reltype traversal") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})")

    val query = "MATCH (n)-->(m) RETURN n.name ORDER BY n.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)
  }

  test("should not find relationship with grant and deny on all reltype traversal") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})")

    val query = "MATCH (n)-->(m) RETURN n.name ORDER BY n.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected = List("a", "b")

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("n.name") should be(expected(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)
  }

  test("should find correct relationships with grant traversal on all reltypes and deny on specific reltype") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})")

    val query = "MATCH (n)-->(m) RETURN n.name ORDER BY n.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected = List("a", "b")

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("n.name") should be(expected(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS HATES TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should find correct relationships with grant and deny traversal on specific reltypes") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})")

    val query = "MATCH (n)-->(m) RETURN n.name ORDER BY n.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS HATES TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should get correct count for all relationships with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)
  }

  test("should get correct count for specific relationship with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)
  }

  test("should get relationships for a matched node") {

    import scala.collection.JavaConverters._
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:Start), (a)-[:A]->(), (a)-[:B]->()")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(0)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set())
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(1)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A"))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(2)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A", "B"))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(1)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("B"))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(0)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set())
    }) should be(1)
  }

  test("should get correct relationship types and count from procedure") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    val query = "CALL db.relationshipTypes YIELD relationshipType as reltype, relationshipCount as count"

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A), (a)-[:A]->(:A), (a)-[:B]->(:A), (a)-[:B]->(:B)")

    // WHEN ... THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("reltype") should be("A")
      row.get("count") should be(1)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS B TO custom")

    val expected1 = List(
      ("A", 1),
      ("B", 1)
    )

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    val expected2 = List(
      ("A", 1),
      ("B", 2)
    )

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("reltype") should be("B")
      row.get("count") should be(2)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)
  }

  test("should only see properties on relationship with read privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should equal(util.Collections.emptyMap())
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (id) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L),
      util.Collections.emptyMap()
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected2(index))
    }) should be(2)
  }

  test("should not be able to read properties when denied read privilege for all reltypes and all properties") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should equal(util.Collections.emptyMap())
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ(*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should equal(util.Collections.emptyMap())
    }) should be(2)
  }

  test("should read correct properties when denied read privilege for all reltypes and specific property") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ (foo) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("id", 1L),
      util.Map.of("id", 3L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected2(index))
    }) should be(2)

  }

  test("should read correct properties when denied read privilege for specific reltype and all properties") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ (*) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("id", 3L, "foo", 4L),
      util.Collections.emptyMap()
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected2(index))
    }) should be(2)

  }

  test("should read correct properties when denied read privilege for specific reltype and specific property") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("id", 1L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected2(index))
    }) should be(2)

  }

  test("should read correct properties with several grants and denies on read relationships") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2, bar: 5}]->(),
        |()-[:B {id: 3, foo: 4, bar: 6}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (id) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L),
      util.Map.of("id", 3L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ (id) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("id", 3L),
      util.Collections.emptyMap()
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected3 = List(
      util.Map.of("id", 3L),
      util.Map.of("foo", 2L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected3(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ (bar) ON GRAPH * RELATIONSHIPS B TO custom")

    // THEN
    val expected4 = List(
      util.Map.of("id", 3L),
      util.Map.of("foo", 2L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected4(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected5 = List(
      util.Map.of("id", 3L, "foo", 4L ),
      util.Map.of("foo", 2L, "bar", 5L )
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected5(index))
    }) should be(2)

  }

  test("should see properties and relationships depending on granted MATCH privileges for role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT READ (id) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 1}]->(),
        |()-[:A {id: 2, bar: 1}]->(),
        |()-[:A {id: 3, foo: 0, bar: 0}]->(),
        |()-[:A {id: 4, foo: 0, bar: 1}]->(),
        |()-[:A {id: 5, foo: 1, bar: 1}]->(),
        |()-[:A {id: 6, foo: 1, bar: 0}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() WHERE r.foo = 1 OR r.bar = 1 RETURN r.id, r.foo, r.bar"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected1 = List(
      (1, 1, null),
      (5, 1, null),
      (6, 1, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected1(index))
    }) should be(3)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      (1, 1, null),
      (2, null, 1),
      (4, 0, 1),
      (5, 1, 1),
      (6, 1, 0)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected2(index))
    }) should be(5)
  }

  test("should see properties and relationships depending on granted MATCH privileges for role fulltext index") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT READ (id) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE
        |()-[:A {id: 1, foo: 'true'}]->(),
        |()-[:A {id: 2, bar: 'true'}]->(),
        |()-[:A {id: 3, foo: 'false', bar: 'false'}]->(),
        |()-[:A {id: 4, foo: 'false', bar: 'true'}]->(),
        |()-[:A {id: 5, foo: 'true', bar: 'true'}]->(),
        |()-[:A {id: 6, foo: 'true', bar: 'false'}]->()
      """.stripMargin)

    execute("CALL db.index.fulltext.createRelationshipIndex('relIndex', ['A'], ['foo', 'bar'])")
    execute("CALL db.awaitIndexes()")

    val query = "CALL db.index.fulltext.queryRelationships('relIndex', 'true') YIELD relationship AS r RETURN r.id, r.foo, r.bar ORDER BY r.id"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected = List(
      (1, "true", null),
      (2, null, "true"),
      (4, "false", "true"),
      (5, "true", "true"),
      (6, "true", "false")
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected(index))
    }) should be(5)
  }

  test("should give correct results with relationship") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:A {prop: 'string with words in it'}]->(:A),
        |(:A)-[:B {prop: 'another string with words in it'}]->(:A),
        |(:C)-[:B {prop: 'this string contains words'}]->(:A),
        |(:A)-[:B {prop: 'this is just a string'}]->(:A),
        |()-[:A {prop: 'words words words'}]->(:A)
      """.stripMargin)

    val query = "MATCH ()-[r]->() WHERE r.prop contains 'words' RETURN r.prop AS prop"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    val expected1 = List(
      "string with words in it",
      "another string with words in it"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES C TO custom")

    // THEN
    val expected2 = List(
      "string with words in it",
      "another string with words in it",
      "this string contains words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)
  }

  test("should give correct results with relationship fulltext index") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:A {prop: 'string with words in it'}]->(:A),
        |(:A)-[:B {prop: 'another string with words in it'}]->(:A),
        |(:C)-[:B {prop: 'this string contains words'}]->(:A),
        |(:A)-[:B {prop: 'this is just a string'}]->(:A),
        |()-[:A {prop: 'words words words'}]->(:A)
      """.stripMargin)

    execute("CALL db.index.fulltext.createRelationshipIndex('relIndex', ['A', 'B'], ['prop'])")
    execute("CALL db.awaitIndexes()")

    val query = "CALL db.index.fulltext.queryRelationships('relIndex', 'words') YIELD relationship RETURN relationship.prop AS prop"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    val expected1 = List(
      "string with words in it",
      "another string with words in it"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES C TO custom")

    // THEN
    val expected2 = List(
      "this string contains words",
      "string with words in it",
      "another string with words in it"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)
  }

  test("should not be able to read properties when denied match privilege for all reltypes and all properties") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)
  }

  test("should read correct properties when denied match privilege for all reltypes and specific property") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH (foo) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

  }

  test("should read correct properties when denied match privilege for specific reltype and all properties") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH (*) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should be(util.Map.of("id", 3L, "foo", 4L))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should be(util.Map.of("id", 3L, "foo", 4L))
    }) should be(1)

  }

  test("should read correct properties when denied match privilege for specific reltype and specific property") {

    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("id", 3L, "foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should be(util.Map.of("id", 3L, "foo", 4L))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should be(util.Map.of("id", 3L, "foo", 4L))
    }) should be(1)

  }

  test("should give correct results with relationship fulltext index and denies") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * ELEMENTS * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:A {prop: 'string with words in it'}]->(:A),
        |(:A)-[:B {prop: 'another string with words in it'}]->(:A),
        |(:C)-[:B {prop: 'this string contains words'}]->(:A),
        |(:A)-[:B {prop: 'this is just a string'}]->(:A),
        |()-[:A {prop: 'words words words'}]->(:A)
      """.stripMargin)

    execute("CALL db.index.fulltext.createRelationshipIndex('relIndex', ['A', 'B'], ['prop'])")
    execute("CALL db.awaitIndexes()")

    val query = "CALL db.index.fulltext.queryRelationships('relIndex', 'words') YIELD relationship RETURN relationship.prop AS prop ORDER BY prop"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * TO custom" )

    // THEN
    val expected1 = List(
      "another string with words in it",
      "string with words in it",
      "this string contains words",
      "words words words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected1(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES C TO custom" )

    // THEN
    val expected2 = List(
      "another string with words in it",
      "string with words in it",
      "words words words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)


    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ (prop) ON GRAPH * RELATIONSHIPS A TO custom" )

    // THEN
    val expected3 = List(
      "another string with words in it",
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected3(index))
    }) should be(1)


    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS B TO custom" )

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("Should get no result")
    }) should be(0)

  }

  // Mixed tests

  test("should get correct count within transaction for restricted user") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT WRITE (*) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B), (:B)")

    val countQuery = "MATCH (n:A) RETURN count(n) as count"
    val createAndCountQuery = "CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as count"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", createAndCountQuery, resultHandler = (row, _) => {
      row.get("count") should be(3) // commited (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node
    }) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 3)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", createAndCountQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // commited one more, and allowed traverse on all labels (but not matching on B)
    }) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 4)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", createAndCountQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // Commited one more, but disallowed B so (:A:B) disappears
    }) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 5)))

  }

  test("should get correct count within transaction for restricted user using count store") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT WRITE (*) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B), (:B)")

    val countQuery = "MATCH (n:A) RETURN count(n) as count"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", countQuery, resultHandler = (row, _) => {
      row.get("count") should be(3) // commited (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node
    }, executeBefore = () => createLabeledNode("A")) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 3)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", countQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // commited one more, and allowed traverse on all labels (but not matching on B)
    }, executeBefore = () => createLabeledNode("A")) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 4)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", countQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // Commited one more, but disallowed B so (:A:B) disappears
    }, executeBefore = () => createLabeledNode("A")) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 5)))

  }

  test("should support whitelist and blacklist traversal in index seeks") {
    setupMultilabelData
    graph.createIndex("A", "foo")
    setupUserWithCustomRole("user1", "secret", "role1")
    setupUserWithCustomRole("user2", "secret", "role2")
    setupUserWithCustomRole("user3", "secret", "role3")

    selectDatabase(SYSTEM_DATABASE_NAME)

    // role1 whitelist A
    execute("GRANT READ (foo) ON GRAPH * NODES * TO role1")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO role1")

    // role2 whitelist A and blacklist B
    execute("GRANT READ (foo) ON GRAPH * NODES * TO role2")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO role2")
    execute("DENY TRAVERSE ON GRAPH * NODES B TO role2")

    // role3 whitelist all labels and blacklist B
    execute("GRANT READ (foo) ON GRAPH * NODES * TO role3")
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

  test("should rollback transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    val tx = graph.beginTransaction(Transaction.Type.explicit, LoginContext.AUTH_DISABLED)
    try {
      val result: Result = new RichGraphDatabaseQueryService(graph).execute("GRANT TRAVERSE ON GRAPH * NODES A,B TO custom")
      result.accept(_ => true)
      tx.failure()
    } finally {
      tx.close()
    }
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  // helper variable, methods and class

  private def setupMultilabelData = {
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
