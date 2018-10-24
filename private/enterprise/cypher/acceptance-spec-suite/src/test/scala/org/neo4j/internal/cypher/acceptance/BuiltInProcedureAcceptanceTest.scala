/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.{Label, Node, Relationship}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.scalatest.LoneElement._

import scala.collection.JavaConversions._

class BuiltInProcedureAcceptanceTest extends ProcedureCallAcceptanceTest with CypherComparisonSupport {

  test("should be able to filter as part of call") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = executeWith(Configs.InterpretedAndSlotted, "CALL db.labels() YIELD label WHERE label <> 'A' RETURN *")

    // ThenBuiltInProceduresIT.java:136
    result.toList should equal(
      List(
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("should be able to use db.schema") {

    // Given
    val neo = createLabeledNode("Neo")
    val d1 = createLabeledNode("Department")
    val e1 = createLabeledNode("Employee")
    relate(e1, d1, "WORKS_AT", "Hallo")
    relate(d1, neo, "PART_OF", "Hallo")

    // When
    // we cannot assert on the results because on each call
    // the generated virtual nodes will have different IDs
    val result = executeWith(Configs.All, "CALL db.schema()", expectedDifferentResults = Configs.All).toList

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

  test("should be able to use db.schema.visualization") {

    // Given
    val neo = createLabeledNode("Neo")
    val d1 = createLabeledNode("Department")
    val e1 = createLabeledNode("Employee")
    relate(e1, d1, "WORKS_AT", "Hallo")
    relate(d1, neo, "PART_OF", "Hallo")

    // When
    val result = executeWith(Configs.All, "CALL db.schema.visualization()", expectedDifferentResults = Configs.All).toList

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

  test("yielding db.schema() and inspecting properties should give empty results") {
    val a = createLabeledNode(Map("name" -> "ajax"), "A")
    val b = createLabeledNode(Map("name" -> "beetle"), "B")
    relate(a, b, "R")

    val query = "CALL db.schema() YIELD nodes UNWIND nodes AS node RETURN properties(node) AS props"
    val result = executeSingle(query).toList
    result should be(
      List(
        Map("props" -> Map()),
        Map("props" -> Map())
      )
    )
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
    val result = executeWith(Configs.InterpretedAndSlotted, "CALL db.labels() YIELD label RETURN *")

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
    val result = executeWith(Configs.InterpretedAndSlotted, "MATCH (n {name: 'Toc'}) WITH n.name AS name CALL db.labels() YIELD label RETURN *")

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
    val result = executeWith(Configs.InterpretedAndSlotted, "CALL db.labels() YIELD label, count RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A", "count" -> 3),
        Map("label" -> "B", "count" -> 1),
        Map("label" -> "C", "count" -> 1)))
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
    val result = executeWith(Configs.InterpretedAndSlotted, "CALL db.labels() YIELD label, count RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A", "count" -> 3),
        Map("label" -> "B", "count" -> 1)))
  }

  test("db.labels works on an empty database") {
    // Given an empty database
    //When
    val result = executeWith(Configs.InterpretedAndSlotted, "CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels work on an empty database") {
    // Given an empty database
    //When
    val result = executeWith(Configs.All, "CALL db.labels")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels should be empty when all labels are removed") {
    // Given
    createLabeledNode("A")
    execute("MATCH (a:A) REMOVE a:A")

    //When
    val result = executeWith(Configs.All, "CALL db.labels")

    // Then
    result shouldBe empty
  }

  test("db.labels should be empty when all nodes are removed") {
    // Given
    createLabeledNode("A")
    execute("MATCH (a) DETACH DELETE a")

    //When
    val result = executeWith(Configs.All, "CALL db.labels")

    // Then
    result shouldBe empty
  }

  test("should be able to find types from built-in-procedure") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")

    // When
    val result = executeWith(Configs.All, "CALL db.relationshipTypes")

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A", "count" -> 1),
        Map("relationshipType" -> "B", "count" -> 2),
        Map("relationshipType" -> "C", "count" -> 1)))
  }

  test("db.relationshipType work on an empty database") {
    // Given an empty database
    //When
    val result = executeWith(Configs.All, "CALL db.relationshipTypes")

    // Then
    result shouldBe empty
  }

  test("db.relationshipTypes should be empty when all relationships are removed") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")
    execute("MATCH (a) DETACH DELETE a")

    //When
    val result = executeWith(Configs.All, "CALL db.relationshipTypes")

    // Then
    result shouldBe empty
  }

  test("should be able to find propertyKeys from built-in-procedure") {
    // Given
    createNode("A" -> 1, "B" -> 2, "C" -> 3)

    // When
    val result = executeWith(Configs.All, "CALL db.propertyKeys")

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
    val result = executeWith(Configs.All, "CALL db.propertyKeys")

    // Then
    result shouldBe empty
  }

  test("removing properties from nodes and relationships does not remove them from the store") {
    // Given
    relate(createNode("A" -> 1), createNode("B" -> 1), "R" ->1)
    execute("MATCH (a)-[r]-(b) REMOVE a.A, r.R, b.B")

    // When
    val result = executeWith(Configs.All, "CALL db.propertyKeys")

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
    val result = executeWith(Configs.All, "CALL db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKey" -> "A"),
        Map("propertyKey" -> "B"),
        Map("propertyKey" -> "R")))
  }

  test("should be able to find indexes from built-in-procedure") {
    // Given
    graph.createIndex("A", "prop")

    //When
    val result = executeWith(Configs.All, "CALL db.indexes")

    // Then
    result.toList should equal(
      List(Map("description" -> "INDEX ON :A(prop)",
        "indexName" -> "Unnamed index",
        "tokenNames" -> List("A"),
        "properties" -> List("prop"),
        "state" -> "ONLINE",
        "progress" -> 100D,
        "type" -> "node_label_property",
        "id" -> 1,
        "provider" -> Map(
          "version" -> GenericNativeIndexProvider.DESCRIPTOR.getVersion,
          "key" -> GenericNativeIndexProvider.DESCRIPTOR.getKey),
        "failureMessage" -> "")))
  }

  test("yield from void procedure should return correct error msg") {
    failWithError(Configs.Version4_0 + Configs.Version3_4,
                  "CALL db.createLabel('Label') yield node",
                  List("Cannot yield value from void procedure."))
  }

  test("should create index from built-in-procedure") {
    // when
    val createResult = executeWith(Configs.All, "CALL db.createIndex(\":Person(name)\",\"lucene+native-1.0\")")

    // then
    createResult.toList should equal(
      List(Map(
        "index" -> ":Person(name)",
        "providerName" -> "lucene+native-1.0",
        "status" -> "index created"))
    )

    graph.execute("CALL db.awaitIndexes(10)")

    // when
    val listResult = executeWith(Configs.All, "CALL db.indexes()")

    // Then
    listResult.toList should equal(
      List(Map("description" -> "INDEX ON :Person(name)",
        "indexName" -> "Unnamed index",
        "tokenNames" -> List("Person"),
        "properties" -> List("name"),
        "state" -> "ONLINE",
        "progress" -> 100D,
        "type" -> "node_label_property",
        "id" -> 1,
        "provider" -> Map(
          "version" -> "1.0",
          "key" -> "lucene+native"),
        "failureMessage" -> "" )))
  }

  test("should create unique property constraint from built-in-procedure") {
    // when
    val createResult = executeWith(Configs.All, "CALL db.createUniquePropertyConstraint(\":Person(name)\",\"lucene+native-1.0\")")

    // then
    createResult.toList should equal(
      List(Map(
        "index" -> ":Person(name)",
        "providerName" -> "lucene+native-1.0",
        "status" -> "uniqueness constraint online"))
    )

    graph.execute("CALL db.awaitIndexes(10)")

    // when
    val mapResult = executeWith(Configs.All, "CALL db.indexes()").toList.loneElement

    // then
    mapResult should have size 10
    mapResult("description") should equal("INDEX ON :Person(name)")
    mapResult("indexName").asInstanceOf[String] should startWith("index_")
    mapResult("id").asInstanceOf[Long].intValue() should be >= 1
    mapResult("tokenNames") should equal(List("Person"))
    mapResult("properties") should equal(List("name"))
    mapResult("state") should equal("ONLINE")
    mapResult("progress") should equal(100D)
    mapResult("type") should equal("node_unique_property")
    mapResult("provider") should equal(Map(
               "version" -> "1.0",
                "key" -> "lucene+native"))
    mapResult("failureMessage") should equal("")
  }

  test("should create node key constraint from built-in-procedure") {
    // when
    val createResult = executeWith(Configs.All, "CALL db.createNodeKey(\":Person(name)\",\"lucene+native-1.0\")")

    // then
    createResult.toList should equal(
      List(Map(
        "index" -> ":Person(name)",
        "providerName" -> "lucene+native-1.0",
        "status" -> "node key constraint online"))
    )

    graph.execute("CALL db.awaitIndexes(10)")

    // when
    val mapResult = executeWith(Configs.All, "CALL db.indexes()").toList.loneElement

    // then
    mapResult should have size 10
    mapResult("description") should equal("INDEX ON :Person(name)")
    mapResult("indexName").asInstanceOf[String] should startWith("index_")
    mapResult("id").asInstanceOf[Long].intValue() should be >= 1
    mapResult("tokenNames") should equal(List("Person"))
    mapResult("properties") should equal(List("name"))
    mapResult("state") should equal("ONLINE")
    mapResult("progress") should equal(100D)
    mapResult("type") should equal("node_unique_property")
    mapResult("provider") should equal(Map(
      "version" -> "1.0",
      "key" -> "lucene+native"))
    mapResult("failureMessage") should equal("")
  }

  test("should list indexes in alphabetical order") {
    // Given
    graph.createIndex("A", "prop")
    graph.createIndex("C", "foo")
    graph.createIndex("B", "foo")
    graph.createIndex("A", "foo")
    graph.createIndex("A", "bar")

    //When
    val result = executeWith(Configs.InterpretedAndSlotted, "CALL db.indexes() YIELD description RETURN description")

    // Then
    result.columnAs("description").toList should equal(
      List("INDEX ON :A(bar)", "INDEX ON :A(foo)", "INDEX ON :A(prop)", "INDEX ON :B(foo)", "INDEX ON :C(foo)"))
  }
}
