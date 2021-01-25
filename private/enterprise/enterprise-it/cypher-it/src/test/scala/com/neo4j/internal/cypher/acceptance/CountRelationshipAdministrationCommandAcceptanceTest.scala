/*
 * Copyright (c) "Neo4j"
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
  private val returnCountVar = "count"
  private val countAllRelationshipsQuery = s"MATCH ()-[r]->() RETURN count(r) as $returnCountVar"
  private val countLovesRelationshipsQuery = s"MATCH ()-[r:LOVES]->() RETURN count(r) as $returnCountVar"

  def setupGraph(): Unit = {
    setupUserWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.withTx(tx => tx.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})"))

    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  test("should get no relationships with only match privileges") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countAllRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(0)
    }) should be(1)
  }

  test("should get subset of relationships given limited node traversal privileges") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countAllRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(1) // Only see (:A)-[]->(:A), not (:A)-[]->(:B) since no traverse node B privilege
    }) should be(1)
  }

  test("should get all relationships with unspecific node match privileges") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countAllRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(2)
    }) should be(1)
  }

  test("should get no relationships after denied traverse privileges") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * RELATIONSHIPS * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countAllRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(0)
    }) should be(1)
  }

  test("should get no relationships with only specific match privileges") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countLovesRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(0)
    }) should be(1)
  }

  test("should get partial relationships from specific traversal privileges for subset of nodes") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countLovesRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(1) // Only see (:A)-[]->(:A), not (:A)-[]->(:B) since no traverse node B privilege
    }) should be(1)
  }

  test("should get all relationships from specific traversal privileges for all nodes") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES A, B TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countLovesRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(2)
    }) should be(1)
  }

  test("should get no relationships from specific traversal privileges once denied") {
    // GIVEN
    setupGraph()

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, countAllRelationshipsQuery, resultHandler = (row, _) => {
      row.get(returnCountVar) should be(0) // Denied relationship LOVES traverse, no other relationships exists in graph
    }) should be(1)
  }

  test("should get zero count for relationships with no traverse relationship privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * NODES * TO $roleName")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("No TRAVERSE on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> ((0, denseTestHelper.superCount)),
        denseTestHelper.sizeOfAllSparseNode -> ((0, denseTestHelper.superCount)),
        denseTestHelper.sizeOfLovesDenseNode -> ((0, 1)),
        denseTestHelper.sizeOfAllDenseNode -> ((0, 1)),
        denseTestHelper.countLovesDenseNode -> ((0, 1)),
        denseTestHelper.countAllDenseNode -> ((0, 1))
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on LOVES relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * NODES * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO $roleName")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("TRAVERSE LOVES on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> ((1, denseTestHelper.superCount)),
        denseTestHelper.sizeOfAllSparseNode -> ((1, denseTestHelper.superCount)),
        denseTestHelper.sizeOfLovesDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.sizeOfAllDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.countLovesDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.countAllDenseNode -> ((denseTestHelper.superCount, 1))
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on all relationships") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * NODES * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES, KNOWS TO $roleName") // equivalent to 'RELATIONSHIPS *' on the given graph
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("TRAVERSE * on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> ((1, denseTestHelper.superCount)),
        denseTestHelper.sizeOfAllSparseNode -> ((2, denseTestHelper.superCount)),
        denseTestHelper.sizeOfLovesDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.sizeOfAllDenseNode -> ((denseTestHelper.superCount * 2, 1)),
        denseTestHelper.countLovesDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.countAllDenseNode -> ((denseTestHelper.superCount * 2, 1))
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on all relationships, deny traverse C nodes") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * NODES * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES, KNOWS TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES C TO $roleName")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("deny TRAVERSE C on nodes:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> ((1, denseTestHelper.superCount)),
        denseTestHelper.sizeOfAllSparseNode -> ((1, denseTestHelper.superCount)),
        denseTestHelper.sizeOfLovesDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.sizeOfAllDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.countLovesDenseNode -> ((denseTestHelper.superCount, 1)),
        denseTestHelper.countAllDenseNode -> ((denseTestHelper.superCount, 1))
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationships with traverse on KNOWS relationships, deny traverse C nodes") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * NODES * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES, KNOWS TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES C TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO $roleName")
    denseTestHelper.setupGraph()

    // WHEN .. THEN
    withClue("deny TRAVERSE LOVES on relationship:") {
      Seq(
        denseTestHelper.sizeOfLovesSparseNode -> ((0, denseTestHelper.superCount)),
        denseTestHelper.sizeOfAllSparseNode -> ((0, denseTestHelper.superCount)),
        denseTestHelper.sizeOfLovesDenseNode -> ((0, 1)),
        denseTestHelper.sizeOfAllDenseNode -> ((0, 1)),
        denseTestHelper.countLovesDenseNode -> ((0, 1)),
        denseTestHelper.countAllDenseNode -> ((0, 1))
      ).foreach(denseTestHelper.testCounts)
    }
  }

  test("should get correct count for relationship within transaction with traversal privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT WRITE ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:R]->(:A:B)<-[:R]-(:B)")

    val countQuery = s"MATCH ()-[r:R]->(:A) RETURN count(r) as $returnCountVar"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS R TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(countQuery).toList should be(List(Map(returnCountVar -> 2)))

    // THEN
    executeOnDBMSDefault(username, password, countQuery, requiredOperator = Some("RelationshipCountFromCountStore"), resultHandler = (row, _) => {
      row.get(returnCountVar) should be(3) // Can see (:A)-[:R]->(:A:B) and the two in TX, not (:A:B)<-[:R]-(:B) since no traverse node B privilege
    }, executeBefore = tx => tx.execute("CREATE (:A)-[:R]->(:A)<-[:R]-(:B)")) should be(1)

    execute(countQuery).toList should be(List(Map(returnCountVar -> 4)))
  }

  test("Counting queries should work with restricted user") {
    // The two queries are used by the browser
    val countingNodesQuery = "MATCH () RETURN { name:'nodes', data:count(*) } AS result"
    val countingRelsQuery = "MATCH ()-[]->() RETURN { name:'relationships', data: count(*)} AS result"

    // Given graph and default privileges
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Person)-[:WROTE]->(:Letter)<-[:HAS_STAMP]-(:Stamp)")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER tim SET PASSWORD '123' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO tim")
    execute("GRANT ACCESS ON DATABASE * TO role")
    execute("GRANT MATCH {*} ON GRAPH * ELEMENTS * TO role")

    // RELS
    // Given
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIP WROTE TO role")
    selectDatabase(DEFAULT_DATABASE_NAME)

    // When & Then

    // unrestricted:
    execute(countingRelsQuery).toList should be(List(Map("result" -> Map("data" -> 2, "name" -> "relationships"))))

    // restricted
    executeOnDBMSDefault("tim", "123", countingRelsQuery,
      resultHandler = (row, _) => {
        val result = row.get("result").asInstanceOf[util.Map[String, AnyRef]]
        result.get("data") should be(1L)
        result.get("name") should be("relationships")
      }
    ) should be(1)

    // NODES
    // Given
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES Person TO role")
    selectDatabase(DEFAULT_DATABASE_NAME)

    // When & Then

    // unrestricted:
    execute(countingNodesQuery).toList should be(List(Map("result" -> Map("data" -> 3, "name" -> "nodes"))))

    // restricted
    executeOnDBMSDefault("tim", "123", countingNodesQuery,
      resultHandler = (row, _) => {
        val result = row.get("result").asInstanceOf[util.Map[String, AnyRef]]
        result.get("data") should be(2L)
        result.get("name") should be("nodes")
      }
    ) should be(1)
  }

  object denseTestHelper {
    val superCount = 100
    val sizeOfLovesSparseNode = s"MATCH (b:B {name:'sparse'}) RETURN size((b)<-[:LOVES]-()) AS $returnCountVar"
    val sizeOfAllSparseNode = s"MATCH (b:B {name:'sparse'}) RETURN size((b)<--()) AS $returnCountVar"
    val sizeOfLovesDenseNode = s"MATCH (a:A {name:'dense'}) RETURN size((a)-[:LOVES]->()) AS $returnCountVar"
    val sizeOfAllDenseNode = s"MATCH (a:A {name:'dense'}) RETURN size((a)-->()) AS $returnCountVar"
    val countLovesDenseNode = s"MATCH (:A {name:'dense'})-[r:LOVES]->() RETURN count(r) AS $returnCountVar"
    val countAllDenseNode = s"MATCH (:A {name:'dense'})-[r]->() RETURN count(r) AS $returnCountVar"
    val testCounts: PartialFunction[AnyRef, Unit] = {
      case (query: String, (expectedCount: Int, expectedRows: Int)) =>
        withClue(s"$query:") {
          executeOnDBMSDefault(username, password, query, resultHandler = (row, _) => {
            withClue("result should be:") {
              row.get(returnCountVar) should be(expectedCount)
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
