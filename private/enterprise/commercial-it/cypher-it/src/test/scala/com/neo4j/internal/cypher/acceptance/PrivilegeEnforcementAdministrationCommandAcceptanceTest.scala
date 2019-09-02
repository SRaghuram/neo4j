/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.{Label, Node, Result}
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext

// Tests for actual behaviour of authorization rules for restricted users based on privileges
class PrivilegeEnforcementAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Tests for node privileges

  test("should match nodes when granted traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (n:A {name:'a'})"))
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

  test("should read properties when granted MATCH privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (n:A {name:'a'})"))

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT READ {name} ON GRAPH * NODES A (*) FROM custom")

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
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

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
    execute("GRANT READ {name} ON GRAPH * NODES A (*) TO custom")

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

  test("read privilege for element should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name: 'n1'})-[:A {name:'r'}]->(:A {name: 'n2'})")
    val query = "MATCH (n1)-[r]->(n2) RETURN n1.name, r.name, n2.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {name} ON GRAPH * ELEMENTS A (*) TO custom")

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
    execute("GRANT READ {name} ON GRAPH * ELEMENTS A, B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(("a1", "ra1", "a2"))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {name} ON GRAPH * ELEMENTS B TO custom")

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
    execute("REVOKE READ {name} ON GRAPH * ELEMENTS A FROM custom")

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
    execute("REVOKE READ {name} ON GRAPH * ELEMENTS B FROM custom")

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
    setupMultiLabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role1")
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT READ {foo} ON GRAPH * NODES A (*) TO role2")
    execute("GRANT READ {bar} ON GRAPH * NODES B (*) TO role2")

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

  test("should see properties and nodes depending on granted MATCH privileges for role") {
    // GIVEN
    setupMultiLabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT MATCH {foo} ON GRAPH * NODES A (*) TO role2")
    execute("GRANT MATCH {bar} ON GRAPH * NODES B (*) TO role2")

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
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
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
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)
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

  test("should get correct labels from procedure") {
    // GIVEN
    setupUserWithCustomRole()

    // Currently you need to have some kind of traverse or read access to be able to call the procedure at all
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS ignore TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B:E), (:B:C), (:C:D)")

    val query = "CALL db.labels() YIELD label, nodeCount as count RETURN label, count ORDER BY label"

    // WHEN..THEN
    val expectedZero = List(("A", 0), ("B", 0), ("C", 0),("D", 0),("E", 0))
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("label"), row.get("count")) should be(expectedZero(index))
    }) should be(5)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    val expected = List(("A", 2), ("B", 1), ("C", 0),("D", 0),("E", 1))
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("label"), row.get("count")) should be(expected(index))
    }) should be(5)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    val expectedWithoutB = List(("A", 1), ("C", 0),("D", 0),("E", 0))
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("label"), row.get("count"))  should be(expectedWithoutB(index))
    }) should be(4)

    // WHEN
    graph.createIndex("B","foo")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("label"), row.get("count"))  should be(expectedWithoutB(index))
    }) should be(4)
  }

  Seq(
    "CALL db.relationshipTypes",
    "CALL db.relationshipTypes() YIELD relationshipType, relationshipCount RETURN relationshipType, relationshipCount"
  ).foreach { query =>
    test(s"should get correct types from procedure: $query") {
      // GIVEN
      setupUserWithCustomRole()

      // Currently you need to have some kind of traverse or read access to be able to call the procedure at all
      execute("GRANT TRAVERSE ON GRAPH * NODES ignore TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (:A)-[:REL]->(:A:B:E)<-[:ON]-(:B:C)-[:ON]->(:C:D)")

      // WHEN..THEN
      val expectedBothButEmpty = Map("ON" -> 0, "REL" -> 0)
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        val relType = row.get("relationshipType").toString
        row.get("relationshipCount") should be(expectedBothButEmpty(relType))
      }) should be(2)

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS REL TO custom")

      // Relationships still cannot be counted if their nodes are not also visible
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        val relType = row.get("relationshipType").toString
        row.get("relationshipCount") should be(expectedBothButEmpty(relType))
      }) should be(2)

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

      // THEN
      val expected = Map("ON" -> 0, "REL" -> 1)
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        val relType = row.get("relationshipType").toString
        row.get("relationshipCount") should be(expected(relType))
      }) should be(2)

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS `ON` TO custom")

      // THEN
      val expectedBoth = Map("ON" -> 2, "REL" -> 1)
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        val relType = row.get("relationshipType").toString
        row.get("relationshipCount") should be(expectedBoth(relType))
      }) should be(2)

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS `ON` TO custom")

      // THEN
      val expectedRel = Map("REL" -> 1)
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        val relType = row.get("relationshipType").toString
        row.get("relationshipCount") should be(expectedRel(relType))
      }) should be(1)

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("REVOKE DENY TRAVERSE ON GRAPH * RELATIONSHIPS `ON` FROM custom")
      execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

      // THEN
      executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
        fail("result should be empty with deny on all relationships")
      }) should be(0)

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("REVOKE DENY TRAVERSE ON GRAPH * RELATIONSHIPS * FROM custom")

      // THEN
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        val relType = row.get("relationshipType").toString
        row.get("relationshipCount") should be(expectedBoth(relType))
      }) should be(2)
    }
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
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)
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
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom")

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

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)
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

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES * (*) TO custom")

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
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        (row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(2)
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
    )

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (row, index) => {
        row.get("props") should be(expected2(index))
      }) should be(2)
  }

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

  // Tests for relationship privileges

  test("should find relationship when granted traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})"))

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
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})"))

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
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})"))

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
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})"))

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
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})-[:HATES]->(:A {name:'c'})"))

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
    execute("GRANT MATCH {*} ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})"))
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
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")

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
    execute("GRANT MATCH {*} ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})"))
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
    execute("GRANT MATCH {*} ON GRAPH * NODES B TO custom")

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
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx( tx => tx.execute("CREATE (a:Start), (a)-[:A]->(), (a)-[:B]->()"))

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
    graph.withTx( tx => tx.execute("CREATE (a:A), (a)-[:A]->(:A), (a)-[:B]->(:A), (a)-[:B]->(:B)"))

    val expectedZero = List(
      ("A", 0),
      ("B", 0)
    )
    // WHEN ... THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expectedZero(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expectedA = List(
      ("A", 1),
      ("B", 0)
    )
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expectedA(index))
    }) should be(2)

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
    val expectedB = List(
      ("B", 2)
    )
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expectedB(index))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      fail("should get no result")
    }) should be(0)
  }

  test("should only see properties using properties() function on relationship with read privilege") {
    setupUserWithCustomRole()
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
    execute("GRANT READ {id} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT READ {foo} ON GRAPH * RELATIONSHIPS * TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be((null, null))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ{*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be((null, null))
    }) should be(2)
  }

  test("should not be able to read properties using properties() function when denied read privilege for all reltypes and all properties") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should equal(util.Collections.emptyMap())
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ{*} ON GRAPH * RELATIONSHIPS * TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected2 = List(
      (1, null),
      (3, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected2(index))
    }) should be(2)

  }

  test("should read correct properties using properties() function when denied read privilege for all reltypes and specific property") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY READ {foo} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("GRANT READ {foo} ON GRAPH * RELATIONSHIPS * TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      (3, 4),
      (null, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected2(index))
    }) should be(2)

  }

  test("should read correct properties using properties() function when denied read privilege for specific reltype and all properties") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY READ {*} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS A TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {foo} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      (1, null),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected2(index))
    }) should be(2)

  }

  test("should read correct properties using properties() function when denied read privilege for specific reltype and specific property") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY READ {foo} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT READ {foo} ON GRAPH * RELATIONSHIPS A TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo, r.bar ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {id} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, null, null),
      (3, null, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo"), row.getNumber("r.bar")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {id} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      (3, null, null),
      (null, null, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo"), row.getNumber("r.bar")) should be(expected2(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {foo} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected3 = List(
      (3, null, null),
      (null, 2, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo"), row.getNumber("r.bar")) should be(expected3(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {bar} ON GRAPH * RELATIONSHIPS B TO custom")

    // THEN
    val expected4 = List(
      (3, null, null),
      (null, 2, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo"), row.getNumber("r.bar")) should be(expected4(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected5 = List(
      (3, 4, null),
      (null, 2, 5)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo"), row.getNumber("r.bar")) should be(expected5(index))
    }) should be(2)

  }

  test("should read correct properties using properties() function with several grants and denies on read relationships") {

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
    execute("GRANT READ {id} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY READ {id} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT READ {foo} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("DENY READ {bar} ON GRAPH * RELATIONSHIPS B TO custom")

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("GRANT READ {id} ON GRAPH * TO custom")

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
    execute("GRANT MATCH {foo} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT MATCH {bar} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT READ {id} ON GRAPH * TO custom")

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
    execute("GRANT MATCH {foo} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {bar} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT READ {prop} ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {prop} ON GRAPH * RELATIONSHIP B TO custom")

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
    execute("GRANT READ {prop} ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {prop} ON GRAPH * RELATIONSHIP B TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)
  }

  test("should not be able to read properties using properties() function when denied match privilege for all reltypes and all properties") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {foo} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

  }

  test("should read correct properties using properties() function when denied match privilege for all reltypes and specific property") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY MATCH {foo} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query,
      resultHandler = (_, _) => {
        fail("should get no result")
      }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * RELATIONSHIPS * TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be((3, 4))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be((3,4))
    }) should be(1)

  }

  test("should read correct properties using properties() function when denied match privilege for specific reltype and all properties") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should be(util.Map.of("id", 3L, "foo", 4L))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

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

    val query = "MATCH ()-[r]->() RETURN r.id, r.foo ORDER BY r.id"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected1 = List(
      (1, 2),
      (3, 4)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {foo} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be((3, 4))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.getNumber("r.id"), row.getNumber("r.foo")) should be((3, 4))
    }) should be(1)

  }

  test("should read correct properties using properties() function when denied match privilege for specific reltype and specific property") {

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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
    execute("DENY MATCH {foo} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should be(util.Map.of("id", 3L, "foo", 4L))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {foo} ON GRAPH * RELATIONSHIPS A TO custom")

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
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom" )

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
    execute("DENY READ {prop} ON GRAPH * RELATIONSHIPS A TO custom" )

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
    execute("GRANT WRITE {*} ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B), (:B)")

    val countQuery = "MATCH (n:A) RETURN count(n) as count"
    val createAndCountQuery = "CREATE (x:A) WITH x MATCH (n:A) RETURN count(n) as count"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", createAndCountQuery, resultHandler = (row, _) => {
      row.get("count") should be(3) // committed (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node
    }) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 3)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", createAndCountQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // committed one more, and allowed traverse on all labels (but not matching on B)
    }) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 4)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", createAndCountQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // Committed one more, but disallowed B so (:A:B) disappears
    }) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 5)))
  }

  test("should get correct count within transaction for restricted user using count store") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT WRITE {*} ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:A:B), (:B)")

    val countQuery = "MATCH (n:A) RETURN count(n) as count"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("joe", "soap", countQuery, resultHandler = (row, _) => {
      row.get("count") should be(3) // commited (:A) and (:A:B) nodes and one in TX, but not the commited (:B) node
    }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 3)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", countQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // commited one more, and allowed traverse on all labels (but not matching on B)
    }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 4)))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", countQuery, resultHandler = (row, _) => {
      row.get("count") should be(4) // Commited one more, but disallowed B so (:A:B) disappears
    }, executeBefore = tx => tx.createNode(Label.label("A"))) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 5)))
  }

  test("should support whitelist and blacklist traversal in index seeks") {
    setupMultiLabelData
    graph.createIndex("A", "foo")
    setupUserWithCustomRole("user1", "secret", "role1")
    setupUserWithCustomRole("user2", "secret", "role2")
    setupUserWithCustomRole("user3", "secret", "role3")

    selectDatabase(SYSTEM_DATABASE_NAME)

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

  test("should rollback transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    val tx = graph.beginTransaction(Transaction.Type.explicit, LoginContext.AUTH_DISABLED)
    try {
      val result: Result = tx.execute("GRANT TRAVERSE ON GRAPH * NODES A,B TO custom")
      result.accept(_ => true)
      tx.rollback()
    } finally {
      tx.close()
    }
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should get correct result from propertyKeys procedure depending on read privileges") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:X]->(:B)<-[:Y]-(:C)")
    execute("MATCH (n:A) SET n.prop1 = 1")
    execute("MATCH (n:A) SET n.prop2 = 2")
    execute("MATCH (n:A) SET n.prop3 = 3")
    execute("MATCH (n:B) SET n.prop3 = 3")
    execute("MATCH (n:B) SET n.prop4 = 4")
    execute("MATCH (n:C) SET n.prop5 = 5")
    execute("MATCH (n:C) SET n.prop1 = 1")
    execute("MATCH ()<-[x:X]-() SET x.prop6 = 6")
    execute("MATCH ()<-[x:Y]-() SET x.prop7 = 7")
    execute("MATCH (n:A) REMOVE n.prop2") // -> unused prop2

    val query = "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey"

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES ignore TO custom")

    // THEN
    val all = List("prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7")
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES A TO custom")

    // THEN
    // expect no change
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop3} ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES C TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop5} ON GRAPH * NODES * TO custom") // won't do anything new because there could be a REL that has this propKey

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop5} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val withoutFive = List("prop1", "prop2", "prop3", "prop4", "prop6", "prop7")
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES * TO custom") // won't do anything new because there could be a REL that has this propKey

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("Should be empty because all properties are denied on everything")
    }) should be(0)
  }

  test("Counting queries should work with restricted user") {
    // The two queries are used by the browser
    val countingNodesQuery = "MATCH () RETURN { name:'nodes', data:count(*) } AS result"
    val countingRelsQuery = "MATCH ()-[]->() RETURN { name:'relationships', data: count(*)} AS result"

    // Given
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Person)-[:WROTE]->(:Letter)<-[:HAS_STAMP]-(:Stamp)")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER tim SET PASSWORD '123' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO tim")
    execute("GRANT MATCH {*} ON GRAPH * ELEMENTS * TO role")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIP WROTE TO role")

    // RELS
    selectDatabase(DEFAULT_DATABASE_NAME)
    // When & Then

    // unrestricted:
    execute(countingRelsQuery).toList should be(List(Map("result" -> Map("data" -> 2, "name" -> "relationships"))))

    // restricted
    executeOnDefault("tim", "123", countingRelsQuery,
      resultHandler = (row, _) => {
        val result = row.get("result").asInstanceOf[util.Map[String, AnyRef]]
        result.get("data") should be (1L)
        result.get("name") should be ("relationships")
      }
    ) should be(1)

    // Given
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES Person TO role")

    // NODES
    selectDatabase(DEFAULT_DATABASE_NAME)
    // When & Then

    // unrestricted:
    execute(countingNodesQuery).toList should be(List(Map("result" -> Map("data" -> 3, "name" -> "nodes"))))

    // restricted
    executeOnDefault("tim", "123", countingNodesQuery,
      resultHandler = (row, _) => {
        val result = row.get("result").asInstanceOf[util.Map[String, AnyRef]]
        result.get("data") should be (2L)
        result.get("name") should be ("nodes")
      }
    ) should be(1)
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
