/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.graphdb.Node

// Tests for actual behaviour of authorization rules for restricted users based on relationship privileges
class RelationshipPrivilegeEnforcementAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

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
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)
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

  // Index tests

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
}
