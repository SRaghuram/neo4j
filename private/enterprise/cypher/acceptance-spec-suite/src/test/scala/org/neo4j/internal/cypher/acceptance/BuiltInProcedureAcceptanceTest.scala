/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.{Label, Node, Relationship}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider

import scala.collection.JavaConversions._

class BuiltInProcedureAcceptanceTest extends ProcedureCallAcceptanceTest with CypherComparisonSupport {

  test("should be able to filter as part of call") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels() YIELD label WHERE label <> 'A' RETURN *")

    // ThenBuiltInProceduresIT.java:136
    result.toList should equal(
      List(
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("should be able to use db.schema.visualization") {

    // Given
    val neo = createLabeledNode("Neo")
    val d1 = createLabeledNode("Department")
    val e1 = createLabeledNode("Employee")
    relate(e1, d1, "WORKS_AT", "Hallo")
    relate(d1, neo, "PART_OF", "Hallo")

    // When
    val result = executeWith(Configs.ProcedureCall, "CALL db.schema.visualization()", expectedDifferentResults = Configs.All).toList

    // Then
    result.size should equal(1)

    // And then nodes
    val nodes = result.head("nodes").asInstanceOf[Seq[Node]]

    val nodeState: Set[(List[Label], Map[String,AnyRef])] =
      nodes.map(n => (n.getLabels.toList, n.getAllProperties.toMap)).toSet

    val empty = new java.util.ArrayList()
    nodeState should equal(
      Set(
        (List(Label.label("Neo")),        Map("indexes" -> empty, "constraints" -> empty, "name" -> "Neo")),
        (List(Label.label("Department")), Map("indexes" -> empty, "constraints" -> empty, "name" -> "Department")),
        (List(Label.label("Employee")),   Map("indexes" -> empty, "constraints" -> empty, "name" -> "Employee"))
      ))

    // And then relationships
    val relationships = result.head("relationships").asInstanceOf[Seq[Relationship]]

    val relationshipState: Set[String] = relationships.map(_.getType.name()).toSet
    relationshipState should equal(Set("WORKS_AT", "PART_OF"))
  }

  test("should not be able to filter as part of standalone call") {
    failWithError(
      Configs.All,
      "CALL db.labels() YIELD label WHERE label <> 'A'",
      List("Cannot use standalone call with WHERE"))
  }

  test("should be able to find labels from built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A"),
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("should be able to find labels from built-in-procedure from within a query") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    //When
    val result = executeWith(Configs.ProcedureCall, "MATCH (n {name: 'Toc'}) WITH n.name AS name CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("name" -> "Toc", "label" -> "A"),
        Map("name" -> "Toc", "label" -> "B"),
        Map("name" -> "Toc", "label" -> "C")))
  }

  test("should get count for labels") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels() YIELD label, nodeCount RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A", "nodeCount" -> 3),
        Map("label" -> "B", "nodeCount" -> 1),
        Map("label" -> "C", "nodeCount" -> 1)))
  }

  test("should get count for labels even when yielded variables are renamed") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels() YIELD label as l, nodeCount as c RETURN l, c")

    // Then
    result.toList should equal(
      List(
        Map("l" -> "A", "c" -> 3),
        Map("l" -> "B", "c" -> 1),
        Map("l" -> "C", "c" -> 1)))
  }

  test("should get correct count for labels when removed incl. zero counts") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    execute("MATCH (c:C) REMOVE c:C")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels() YIELD label, nodeCount RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A", "nodeCount" -> 3),
        Map("label" -> "B", "nodeCount" -> 1),
        Map("label" -> "C", "nodeCount" -> 0)))
  }

  test("should get correct count for labels when removed") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    execute("MATCH (c:C) REMOVE c:C")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels() YIELD label, nodeCount WHERE nodeCount > 0 RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A", "nodeCount" -> 3),
        Map("label" -> "B", "nodeCount" -> 1)))
  }

  test("should get correct count for labels when added in same transaction") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    //When
    val result = executeWith(
      Configs.InterpretedAndSlotted,
      "CREATE(c:C) WITH 1 AS single CALL db.labels() YIELD label, nodeCount RETURN label, nodeCount")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A", "nodeCount" -> 3),
        Map("label" -> "B", "nodeCount" -> 1),
        Map("label" -> "C", "nodeCount" -> 2)))
  }

  test("should get correct count for labels when removed in same transaction") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    //When
    val result = executeWith(
      Configs.InterpretedAndSlotted,
      "MATCH (c:C) REMOVE c:C WITH count(c) AS single CALL db.labels() YIELD label, nodeCount RETURN label, nodeCount")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A", "nodeCount" -> 3),
        Map("label" -> "B", "nodeCount" -> 1),
        Map("label" -> "C", "nodeCount" -> 0)))
  }

  test("db.labels works on an empty database with yield") {
    // Given an empty database
    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels works on an empty database") {
    // Given an empty database
    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels should show unused label even when all nodes are removed") {
    // Given
    createLabeledNode("A")
    execute("MATCH (a) DETACH DELETE a")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.labels")

    // Then
    result.toList should equal(
      List(Map("label" -> "A", "nodeCount" -> 0)))
  }

  test("should be able to find types from built-in-procedure") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")

    // When
    val result = executeWith(Configs.ProcedureCall, "CALL db.relationshipTypes")

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A", "relationshipCount" -> 1),
        Map("relationshipType" -> "B", "relationshipCount" -> 2),
        Map("relationshipType" -> "C", "relationshipCount" -> 1)))
  }

  test("should get correct count for relationship types when added in transaction") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")

    // When
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
        |CREATE ()-[:C]->()
        |WITH 1 as single
        |CALL db.relationshipTypes() YIELD relationshipType, relationshipCount
        |RETURN relationshipType, relationshipCount
      """.stripMargin)

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A", "relationshipCount" -> 1),
        Map("relationshipType" -> "B", "relationshipCount" -> 2),
        Map("relationshipType" -> "C", "relationshipCount" -> 2)))
  }

  test("should get correct count for relationship types when removed in transaction") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")

    // When
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
        |MATCH ()-[c:C]->()
        |DELETE c
        |WITH count(c) as single
        |CALL db.relationshipTypes() YIELD relationshipType, relationshipCount
        |RETURN relationshipType, relationshipCount
      """.stripMargin)

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A", "relationshipCount" -> 1),
        Map("relationshipType" -> "B", "relationshipCount" -> 2),
        Map("relationshipType" -> "C", "relationshipCount" -> 0)))
  }

  test("db.relationshipType work on an empty database") {
    // Given an empty database
    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.relationshipTypes")

    // Then
    result shouldBe empty
  }

  test("db.relationshipTypes should not be empty when all relationships are removed") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")
    execute("MATCH (a) DETACH DELETE a")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.relationshipTypes")

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A", "relationshipCount" -> 0),
        Map("relationshipType" -> "B", "relationshipCount" -> 0),
        Map("relationshipType" -> "C", "relationshipCount" -> 0)))
  }

  test("should be able to find propertyKeys from built-in-procedure") {
    // Given
    createNode("A" -> 1, "B" -> 2, "C" -> 3)

    // When
    val result = executeWith(Configs.ProcedureCall, "CALL db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKey" -> "A"),
        Map("propertyKey" -> "B"),
        Map("propertyKey" -> "C")))
  }

  test("db.propertyKeys works on an empty database") {
    // Given an empty database

    // When
    val result = executeWith(Configs.ProcedureCall, "CALL db.propertyKeys")

    // Then
    result shouldBe empty
  }

  test("removing properties from nodes and relationships does not remove them from the store") {
    // Given
    relate(createNode("A" -> 1), createNode("B" -> 1), "R" ->1)
    execute("MATCH (a)-[r]-(b) REMOVE a.A, r.R, b.B")

    // When
    val result = executeWith(Configs.ProcedureCall, "CALL db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKey" -> "A"),
        Map("propertyKey" -> "B"),
        Map("propertyKey" -> "R")))
  }

  test("removing all nodes and relationship does not remove properties from the store") {
    // Given
    relate(createNode("A" -> 1), createNode("B" -> 1), "R" ->1)
    execute("MATCH (a) DETACH DELETE a")

    // When
    val result = executeWith(Configs.ProcedureCall, "CALL db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKey" -> "A"),
        Map("propertyKey" -> "B"),
        Map("propertyKey" -> "R")))
  }

  test("should be able to find indexes from built-in-procedure") {
    // Given
    val index = graph.createIndex("A", "prop")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.indexes")

    // Then
    val provider = GenericNativeIndexProvider.DESCRIPTOR.name
    result.toList should equal(
      List(Map(
        "id" -> 1,
        "name" -> index.getName,
        "state" -> "ONLINE",
        "populationPercent" -> 100.0,
        "uniqueness" -> "NONUNIQUE",
        "type" -> "BTREE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> List("A"),
        "properties" -> List("prop"),
        "provider" -> provider )))
  }

  test("yield from void procedure should return correct error msg") {
    failWithError(Configs.All,
                  "CALL db.createLabel('Label') yield node",
                  List("Cannot yield value from void procedure."))
  }

  test("should create index from built-in-procedure") {
    // when
    val createResult = executeWith(Configs.ProcedureCall, "CALL db.createIndex(\"MyIndex\", \":Person(name)\",\"lucene+native-3.0\")")

    // then
    createResult.toList should equal(
      List(Map(
        "name" -> "MyIndex",
        "index" -> ":Person(name)",
        "providerName" -> "lucene+native-3.0",
        "status" -> "index created"))
    )

    graph.withTx( tx => tx.execute("CALL db.awaitIndexes(10)"))

    val index = graph.withTx(tx => Iterators.single(tx.kernelTransaction().schemaRead().index(
      SchemaDescriptor.forLabel(tokenReader(tx, t => t.nodeLabel("Person")), tokenReader(tx, t => t.propertyKey("name"))))))
    // when
    val listResult = executeWith(Configs.ProcedureCall, "CALL db.indexes()")

    // Then
    listResult.toList should equal(
      List(Map(
        "id" -> index.getId,
        "name" -> "MyIndex",
        "state" -> "ONLINE",
        "populationPercent" -> 100.0,
        "uniqueness" -> "NONUNIQUE",
        "type" -> "BTREE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> List("Person"),
        "properties" -> List("name"),
        "provider" -> "lucene+native-3.0" )))
  }

  test("should create unique property constraint from built-in-procedure") {
    // when
    val createResult = executeWith(Configs.ProcedureCall, "CALL db.createUniquePropertyConstraint(\"MyConstraint\", \":Person(name)\",\"lucene+native-3.0\")")

    // then
    createResult.toList should equal(
      List(Map(
        "name" -> "MyConstraint",
        "index" -> ":Person(name)",
        "providerName" -> "lucene+native-3.0",
        "status" -> "uniqueness constraint online"))
    )

    graph.withTx( tx => tx.execute("CALL db.awaitIndexes(10)"))
    val index = inTx(tx => Iterators.single(tx.kernelTransaction().schemaRead().index(
      SchemaDescriptor.forLabel(tokenReader(tx, t => t.nodeLabel("Person")), tokenReader(tx, t => t.propertyKey("name"))))))

    // when
    val listResult = executeWith(Configs.ProcedureCall, "CALL db.indexes()")

    // then
    listResult.toList should equal(
      List(Map(
        "id" -> index.getId,
        "name" -> "MyConstraint",
        "state" -> "ONLINE",
        "populationPercent" -> 100.0,
        "uniqueness" -> "UNIQUE",
        "type" -> "BTREE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> List("Person"),
        "properties" -> List("name"),
        "provider" -> "lucene+native-3.0" )))
  }

  test("should create node key constraint from built-in-procedure") {
    // when
    val createResult = executeWith(Configs.ProcedureCall, "CALL db.createNodeKey(\"MyConstraint\", \":Person(name)\",\"lucene+native-3.0\")")

    // then
    createResult.toList should equal(
      List(Map(
        "name" -> "MyConstraint",
        "index" -> ":Person(name)",
        "providerName" -> "lucene+native-3.0",
        "status" -> "node key constraint online"))
    )

    graph.withTx( tx => tx.execute("CALL db.awaitIndexes(10)"))
    val index = inTx(tx => Iterators.single(tx.kernelTransaction().schemaRead().index(
      SchemaDescriptor.forLabel(tokenReader(tx, t => t.nodeLabel("Person")), tokenReader(tx, t => t.propertyKey("name"))))))

    // when
    val listResult = executeWith(Configs.ProcedureCall, "CALL db.indexes()")

    // then
    listResult.toList should equal(
      List(Map(
        "id" -> index.getId,
        "name" -> "MyConstraint",
        "state" -> "ONLINE",
        "populationPercent" -> 100.0,
        "uniqueness" -> "UNIQUE",
        "type" -> "BTREE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> List("Person"),
        "properties" -> List("name"),
        "provider" -> "lucene+native-3.0" )))
  }

  test("should list indexes in alphabetical order") {
    // Given
    graph.createIndexWithName("poppy", "A", "prop")
    graph.createIndexWithName("benny", "C", "foo")
    graph.createIndexWithName("albert", "B", "foo")
    graph.createIndexWithName("charlie", "A", "foo")
    graph.createIndexWithName("xavier", "A", "bar")

    //When
    val result = executeWith(Configs.ProcedureCall, "CALL db.indexes() YIELD name RETURN name")

    // Then
    result.columnAs("name").toList should equal(
      List("albert",
        "benny",
        "charlie",
        "poppy",
        "xavier"))
  }
}
