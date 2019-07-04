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

class PrivilegeEnforcementDDLAcceptanceTest extends DDLAcceptanceTestBase {

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  test("should match nodes when granted traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()

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
    setupUserJoeWithCustomRole()
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
    setupUserJoeWithCustomRole()

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

  test("read privilege for node should not imply traverse privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("read privilege for relationship should not imply traverse privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * RELATIONSHIPS REL (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]-() RETURN r.name") should be(0)
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
    setupUserJoeWithCustomRole()

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

  test("should find relationship when granted traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
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

  test("should get correct count for all relationships with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)
  }

  test("should get correct count for specific relationship with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES B TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)
  }

  test("should get relationships for a matched node") {

    import scala.collection.JavaConverters._
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:Start), (a)-[:A]->(), (a)-[:B]->()")

    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(1)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A"))
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(2)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A", "B"))
    }) should be(1)
  }

  test("should get relationship types and count from procedure") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
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
  }

  test("should only see properties on relationship with read privilege") {
    setupUserJoeWithCustomRole()
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

  test("should see properties and relationships depending on granted MATCH privileges for role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
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
    setupUserJoeWithCustomRole()
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
    setupUserJoeWithCustomRole()
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
    setupUserJoeWithCustomRole()
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
      "string with words in it",
      "another string with words in it",
      "this string contains words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)
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

    setupUserJoeWithCustomRole()

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
}
