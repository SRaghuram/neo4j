/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.Label

// Tests for actual behaviour of count() function on relationships for restricted users
class CountRelationshipAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should get correct count for all relationships with traversal privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx(tx => tx.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})"))
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
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx(tx => tx.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})"))
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

  test("should get zero count for relationships with no traverse relationship privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("No TRAVERSE on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> (0, denseTestHelper.superCount),
        denseTestHelper.sizeOfAllSparseNode -> (0, denseTestHelper.superCount),
        denseTestHelper.sizeOfLovesDenseNode -> (0, 1),
        denseTestHelper.sizeOfAllDenseNode -> (0, 1),
        denseTestHelper.countLovesDenseNode -> (0, 1),
        denseTestHelper.countAllDenseNode -> (0, 1)
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on LOVES relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("TRAVERSE LOVES on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> (1, denseTestHelper.superCount),
        denseTestHelper.sizeOfAllSparseNode -> (1, denseTestHelper.superCount),
        denseTestHelper.sizeOfLovesDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.sizeOfAllDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.countLovesDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.countAllDenseNode -> (denseTestHelper.superCount, 1)
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on all relationships") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES, KNOWS TO custom")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("TRAVERSE * on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> (1, denseTestHelper.superCount),
        denseTestHelper.sizeOfAllSparseNode -> (2, denseTestHelper.superCount),
        denseTestHelper.sizeOfLovesDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.sizeOfAllDenseNode -> (denseTestHelper.superCount * 2, 1),
        denseTestHelper.countLovesDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.countAllDenseNode -> (denseTestHelper.superCount * 2, 1)
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on all relationships, deny traverse C nodes") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES, KNOWS TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES C TO custom")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("deny TRAVERSE C on nodes:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> (1, denseTestHelper.superCount),
        denseTestHelper.sizeOfAllSparseNode -> (1, denseTestHelper.superCount),
        denseTestHelper.sizeOfLovesDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.sizeOfAllDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.countLovesDenseNode -> (denseTestHelper.superCount, 1),
        denseTestHelper.countAllDenseNode -> (denseTestHelper.superCount, 1)
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on KNOWS relationships, deny traverse C nodes") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES, KNOWS TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES C TO custom")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("deny TRAVERSE LOVES on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> (0, denseTestHelper.superCount),
        denseTestHelper.sizeOfAllSparseNode -> (0, denseTestHelper.superCount),
        denseTestHelper.sizeOfLovesDenseNode -> (0, 1),
        denseTestHelper.sizeOfAllDenseNode -> (0, 1),
        denseTestHelper.countLovesDenseNode -> (0, 1),
        denseTestHelper.countAllDenseNode -> (0, 1)
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationship within transaction with traversal privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:R]->(:A:B)<-[:R]-(:B)")

    val countQuery = "MATCH ()-[r:R]->(:A) RETURN count(r) as count"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS R TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(countQuery).toList should be(List(Map("count" -> 2)))

    // THEN
    executeOnDefault("joe", "soap", countQuery, requiredOperator = Some("RelationshipCountFromCountStore"), resultHandler = (row, _) => {
      row.get("count") should be(3)
    }, executeBefore = tx => tx.execute("CREATE (:A)-[:R]->(:A)<-[:R]-(:B)")) should be(1)

    execute(countQuery).toList should be(List(Map("count" -> 4)))
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
    execute("GRANT ACCESS ON DATABASE * TO role")
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
        result.get("data") should be(1L)
        result.get("name") should be("relationships")
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
        result.get("data") should be(2L)
        result.get("name") should be("nodes")
      }
    ) should be(1)
  }

  object denseTestHelper {
    val superCount = 100
    val sizeOfLovesSparseNode = "MATCH (b:B {name:'sparse'}) RETURN size((b)<-[:LOVES]-()) AS count"
    val sizeOfAllSparseNode = "MATCH (b:B {name:'sparse'}) RETURN size((b)<--()) AS count"
    val sizeOfLovesDenseNode = "MATCH (a:A {name:'dense'}) RETURN size((a)-[:LOVES]->()) AS count"
    val sizeOfAllDenseNode = "MATCH (a:A {name:'dense'}) RETURN size((a)-->()) AS count"
    val countLovesDenseNode = "MATCH (:A {name:'dense'})-[r:LOVES]->() RETURN count(r) AS count"
    val countAllDenseNode = "MATCH (:A {name:'dense'})-[r]->() RETURN count(r) AS count"
    val testCounts: PartialFunction[AnyRef, Unit] = {
      case (query: String, (expectedCount: Int, expectedRows: Int)) =>
        withClue(s"$query:") {
          executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
            withClue("result should be:") {
              row.get("count") should be(expectedCount)
            }
          }) should be(expectedRows)
        }
    }

    def setupGraph(): Unit = {
      selectDatabase(DEFAULT_DATABASE_NAME)
      graph.withTx { tx =>
        val a = tx.createNode(Label.label("A"))
        a.setProperty("name", "dense")
        Range(0, superCount).foreach { _ =>
          tx.execute("WITH $a AS a CREATE (a)-[:LOVES]->(:B {name:'sparse'})<-[:KNOWS]-(c:C {name:'c'}), (a)-[:KNOWS]->(c)", util.Map.of("a", a))
        }
      }
    }
  }
}
