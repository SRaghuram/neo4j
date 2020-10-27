/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.coreapi.InternalTransaction

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters.asScalaBufferConverter

class DBProceduresAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = {
    clearPublicRole()
    execute("GRANT EXECUTE PROCEDURES * ON DBMS TO PUBLIC")
  }

  /*
    ------------ db.relationshipTypes ------------
  */

  test("db.relationshipTypes should return empty result without grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()")

    // WHEN & THEN
    val query = "CALL db.relationshipTypes"
    executeOnDBMSDefault("joe", "soap", query ) should be(0)
  }

  test("db.relationshipTypes should return type when granted traverse") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    val query = "CALL db.relationshipTypes"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("relationshipType") should be("A")
    } ) should be(1)
  }

  test("db.relationshipTypes should return type even if it cannot be found by match") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    val query = "CALL db.relationshipTypes"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("relationshipType") should be("A")
    } ) should be(1)
  }

  test("db.relationshipTypes should return granted traverse *") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP * TO custom")

    // THEN
    val query = "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType"
    val expected = List("A", "B")
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("relationshipType") should be(expected(index))
    } ) should be(2)
  }

  test("db.relationshipTypes should not return denied type") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP * TO custom")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    val query = "CALL db.relationshipTypes"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("relationshipType") should be("B")
    } ) should be(1)
  }

  test("db.relationshipTypes should return empty result for user with only read but not traverse") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {prop} ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return empty result for user with only denied read") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop} ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return type with grant traverse and deny read") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A (*) TO custom")
    execute("DENY READ {prop} ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.relationshipTypes()", resultHandler = (row, _) => {
      row.get("relationshipType") should be("A")
    }) shouldBe 1
  }

  test("db.relationshipTypes should return empty result for user with only write") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  test("db.relationshipTypes should return empty result for user with denied write") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A]->()-[:B]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY WRITE ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.relationshipTypes()") shouldBe 0
  }

  /*
    ------------ db.labels ------------
  */

  test("db.labels should return empty result for user without any traverse") {
    // GIVEN
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN & THEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    executeOnDBMSDefault("joe", "soap", query) should be(0)
  }

  test("db.labels should return correct result for user with traverse") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:B)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODE A TO custom")

    // THEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) should be(1)
  }

  test("db.labels should return all labels for user with traverse *") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A), (:B)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODE * TO custom")

    // THEN
    val expected = List( "A", "B" )
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("label") should be(expected(index))
    }) should be(2)
  }

  test("db.labels should return granted label even if it cannot be found by match") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODE * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODE B TO custom")

    // THEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) should be(1)
  }

  test("db.labels should not return not granted label even if it can be found by match") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODE A TO custom")

    // THEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) should be(1)

    executeOnDBMSDefault("joe", "soap", "MATCH (n:A) RETURN labels(n) as labels", resultHandler = (row, _) => {
      row.get("labels") should be(List("A", "B").asJava)
    }) should be(1)
  }

  test("db.labels should not return denied label") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A),(:B)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODE * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODE A TO custom")

    // THEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("label") should be("B")
    }) should be(1)
  }

  test("db.labels should return empty result for indexed label without traverse") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.createIndex("A","foo")

    // WHEN & THEN
    val query = "CALL db.labels()"
    executeOnDBMSDefault("joe", "soap", query) should be(0)
  }

  test("db.labels should return indexed label with traverse") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.createIndex("A","foo")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODE A TO custom")

    // THEN
    val query = "CALL db.labels()"
    executeOnDBMSDefault("joe", "soap", query) should be(1)
  }

  test("db.labels should return empty result for user with only read but not traverse") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {prop} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return empty result for user with only denied read") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return label with grant traverse and deny read") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")
    execute("DENY READ {prop} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.labels()", resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) shouldBe 1
  }

  test("db.labels should return empty result for user with only write") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should return empty result for user with only denied write") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY WRITE ON GRAPH * TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.labels()") shouldBe 0
  }

  test("db.labels should not return unused label after being removed in transaction") {
    // GIVEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODE * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // THEN
    val expected = List( "A", "B" )
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("label") should be(expected(index))
    }) should be(2)

    // WHEN
    val executeBefore: InternalTransaction => Unit = tx => tx.execute("MATCH (n:A:B) REMOVE n:B")

    // THEN
    executeOnDBMSDefault("joe", "soap", query, executeBefore = executeBefore, resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) should be(1)
  }

  test("db.labels should return used label after being set in transaction") {
    // GIVEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODE * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")
    execute("CALL db.createLabel('B')")

    // THEN
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) should be(1)

    // WHEN
    val executeBefore: InternalTransaction => Unit = tx => tx.execute("MATCH (n:A) SET n:B")

    // THEN
    val expected = List( "A", "B" )
    executeOnDBMSDefault("joe", "soap", query, executeBefore = executeBefore, resultHandler = (row, index) => {
      row.get("label") should be(expected(index))
    }) should be(2)
  }

  test("db.labels should not return used but denied label after being set in transaction") {
    // GIVEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODE * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")
    execute("CALL db.createLabel('B')")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODE B TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) should be(1)

    execute(query).toList should be(Seq(Map("label" -> "A")))

    // WHEN
    val executeBefore: InternalTransaction => Unit = tx => tx.execute("MATCH (n:A) SET n:B")

    // THEN
    executeOnDBMSDefault("joe", "soap", query, executeBefore = executeBefore, resultHandler = (row, _) => {
      row.get("label") should be("A")
    }) should be(1)

    execute(query).toList should be(Seq(Map("label" -> "A"), Map("label" -> "B")))
  }

  test("db.labels should not return used but denied label after being created in transaction") {
    // GIVEN
    val query = "CALL db.labels() YIELD label RETURN label ORDER BY label"
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODE * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('A')")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODE A TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", query) should be(0)
    execute(query).toList should be(Seq.empty)

    // WHEN
    val executeBefore: InternalTransaction => Unit = tx => tx.execute("CREATE (:A)")

    // THEN
    executeOnDBMSDefault("joe", "soap", query, executeBefore = executeBefore) should be(0)
    execute(query).toList should be(Seq(Map("label" -> "A")))
  }

  /*
    ------------ db.propertyKeys ------------
  */

  test("db.propertyKeys should return empty result for user without any grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ({a:1})")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with only write") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ({a:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with only denied write") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ({a:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY WRITE ON GRAPH * TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with only traverse grant") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {a:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return empty result for user with denied traverse") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {a:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return correct result for user with only read grant") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {a:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {a} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1
  }

  test("db.propertyKeys should return empty result for user with denied read") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {a:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {a} ON GRAPH * NODES A (*) TO custom")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return correct result for user with match on any label as long as that propertyKey is part of the grant") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {x:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {x} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT MATCH {x} ON GRAPH * NODES A (*) FROM custom")
    execute("GRANT MATCH {x} ON GRAPH * NODES B (*) TO custom")

    // THEN
    // When the transaction is started, there exists no label B,
    // thus the privilege concerning B is not added to the access mode
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('B')")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT MATCH {x} ON GRAPH * NODES B (*) FROM custom")
    execute("GRANT MATCH {x} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1
  }

  test("db.propertyKeys should return correct result for user with match on any label but deny on one") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {x:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    // y should never show up, since it hasn't been added as a token
    execute("GRANT MATCH {x, y} ON GRAPH * NODES * (*) TO custom")
    execute("DENY MATCH {x} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY MATCH {x} ON GRAPH * NODES * (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT READ {x} ON GRAPH * NODES * (*) FROM custom")
    execute("REVOKE GRANT TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  test("db.propertyKeys should return correct result for user with match on any label and type as long as that propertyKey is part of the grant") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A{a:1, c:3})-[:A{b:2, c:3}]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {b} ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 1

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {a} ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 2

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {c} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 3

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {c} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 2

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.propertyKeys()") shouldBe 0
  }

  /*
    ------------ OTHERS ------------
   */

  test("make sure that db.schema.nodeTypeProperties does not leak for user without grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {a:1})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.schema.nodeTypeProperties()") shouldBe 0
  }

  test("make sure that db.schema.nodeTypeProperties return correct result with grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {a:1, b:2}), (:B {a:1, b:2})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {a} ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.schema.nodeTypeProperties()", resultHandler = (row, _) => {
      row.get("propertyName") should be("a")
      row.get("nodeLabels") should equal(List("A").asJava)
    }) shouldBe 1
  }

  test("make sure that db.schema.relTypeProperties does not leak for user without grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A {a:1, b:2}]->()")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.schema.relTypeProperties()") shouldBe 0
  }

  test("make sure that db.schema.relTypeProperties return correct result with grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:A {a:1, b:2}]->()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT MATCH {a} ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    executeOnDBMSDefault("joe", "soap", "CALL db.schema.relTypeProperties()", resultHandler = (row, _) => {
      row.get("relType") should be(":`A`")
      row.get("propertyName") should be("a")
    }) shouldBe 1
  }

  test("make sure that db.schema.visualization does not leak for user without grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {a:1})-[:A {a:1}]->(:B {a:1})")

    // WHEN & THEN
    executeOnDBMSDefault("joe", "soap", "CALL db.schema.visualization()", resultHandler = (row, _) => {
      row.get("relationships") should equal(util.Collections.EMPTY_LIST)
      row.get("nodes") should equal(util.Collections.EMPTY_LIST)
    }) shouldBe 1
  }

  test("db.schema.visualization with grants") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:B)-[:B]->(:A)-[:A]->(:A)-[:A]->(:C)")
    graph.createIndex("A", "prop1")
    graph.createIndex("C", "prop2")
    graph.createUniqueConstraint("B", "prop3")

    // WHEN & THEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A,B TO custom")
    execute("DENY READ {prop1} ON GRAPH * NODES A TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    executeOnDBMSDefault("joe", "soap", "CALL db.schema.visualization()", resultHandler = (row, _) => {
      // check the relationship type and the start and end node labels
      val relationships = row.get("relationships").asInstanceOf[util.ArrayList[Relationship]].asScala
      relationships.map(rel => (rel.getStartNode.getAllProperties.get("name"), rel.getType.name(), rel.getEndNode.getAllProperties.get("name"))) should be(Seq(
        ("A", "A", "A")
      ))

      // check the node label
      val nodes = row.get("nodes").asInstanceOf[util.ArrayList[Node]].asScala
      nodes.map(_.getAllProperties.get("name")) should equal(Seq("A", "B"))

      // users will see all indexes and constraints for the labels which they are allowed to traverse, regardless of read and schema privileges
      nodes.map(_.getAllProperties.get("indexes").asInstanceOf[util.ArrayList[String]].asScala) should equal(List(List("prop1"), List.empty))
      nodes.map(_.getAllProperties.get("constraints").asInstanceOf[util.ArrayList[String]].asScala) should equal(
        List(List.empty, List("Constraint( id=4, name='constraint_6204dd2c', type='UNIQUENESS', schema=(:B {prop3}), ownedIndex=3 )"))
      )

    }) shouldBe 1
  }

  test("should respect return clause") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    val query = "CALL dbms.listQueries() YIELD query RETURN left( query, 40 ) AS shortQuery, 1 as extra"
    execute(query).toList should be(Seq(Map("shortQuery" -> "CALL dbms.listQueries() YIELD query RETU", "extra" -> 1)))
  }
}
