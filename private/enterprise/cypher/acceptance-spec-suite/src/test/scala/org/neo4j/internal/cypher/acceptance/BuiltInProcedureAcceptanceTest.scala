/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class BuiltInProcedureAcceptanceTest extends ProcedureCallAcceptanceTest with CypherComparisonSupport {

  test("should be able to filter as part of call") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.labels() YIELD label WHERE label <> 'A' RETURN *")

    // ThenBuiltInProceduresIT.java:136
    result.toList should equal(
      List(
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("missing parentheses should result in descriptive error message") {
    // When
    val exception = the[TestFailedException] thrownBy{
      executeWith(Configs.ProcedureCallRead, "CALL db.labels YIELD label WHERE label <> 'A' RETURN *")
    }
    // Then
    exception.getCause.getMessage should startWith("Procedure call is missing parentheses: db.labels")
  }

  test("missing argument and parentheses should result in reasonable error message") {
    // When
    val exception = the[TestFailedException] thrownBy{
      executeWith(Configs.ProcedureCallRead, "CALL db.createLabel RETURN 5 as S")
    }
    // Then
    exception.getCause.getMessage should startWith("Procedure call inside a query does not support passing arguments implicitly. " +
      "Please pass arguments explicitly in parentheses after procedure name for db.createLabel")
  }

  test("should be able to use db.schema.visualization") {

    // Given
    val neo = createLabeledNode("Neo")
    val d1 = createLabeledNode("Department")
    val e1 = createLabeledNode("Employee")
    relate(e1, d1, "WORKS_AT", "Hallo")
    relate(d1, neo, "PART_OF", "Hallo")
    graph.createIndex("Neo", "prop1", "prop2")
    graph.createIndex("Neo", "prop3")
    graph.createUniqueConstraint("Department", "prop")

    // When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.schema.visualization()", expectedDifferentResults = Configs.All).toList

    // Then
    result.size should equal(1)

    // And then nodes
    val nodes = result.head("nodes").asInstanceOf[Seq[Node]]

    val labels = nodes.map( n => n.getLabels.asScala.toList).toSet
    labels should equal(Set(List(Label.label("Neo")), List(Label.label("Department")), List(Label.label("Employee"))))

    val nodeState: Set[(String, Set[String], Set[String])] =
      nodes.map(n => (
        n.getAllProperties.get("name").asInstanceOf[String],
        n.getAllProperties.get("indexes").asInstanceOf[java.util.ArrayList[String]].asScala.toSet,
        n.getAllProperties.get("constraints").asInstanceOf[java.util.ArrayList[String]].asScala.toSet
      )).toSet

    nodeState should equal(
      Set(
        ("Neo",        Set("prop1,prop2", "prop3"), Set()),
        ("Department", Set(),                       Set("Constraint( id=4, name='constraint_c63593d6', type='UNIQUENESS', schema=(:Department {prop}), ownedIndex=3 )")),
        ("Employee",   Set(),                       Set())
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
      "Cannot use standalone call with WHERE")
  }

  test("should be able to find labels from built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.labels() YIELD label RETURN *")

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
    val result = executeWith(Configs.ProcedureCallRead, "MATCH (n {name: 'Toc'}) WITH n.name AS name CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("name" -> "Toc", "label" -> "A"),
        Map("name" -> "Toc", "label" -> "B"),
        Map("name" -> "Toc", "label" -> "C")))
  }

  test("should get correct labels when added in same transaction") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")

    //When
    val result = executeWith(
      Configs.InterpretedAndSlottedAndPipelined,
      "CREATE(c:C) WITH 1 AS single CALL db.labels() YIELD label RETURN label")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A"),
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("should get correct labels when removed in same transaction") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "A")
    createLabeledNode(Map("name" -> "Toc"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    //When
    val result = executeWith(
      Configs.InterpretedAndSlotted,
      "MATCH (c:C) REMOVE c:C WITH count(c) AS single CALL db.labels() YIELD label RETURN label")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A"),
        Map("label" -> "B")))
  }

  test("db.labels works on an empty database with yield") {
    // Given an empty database
    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels works on an empty database") {
    // Given an empty database
    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.labels")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels should be empty when all labels are removed") {
    // Given
    createLabeledNode("A")
    execute("MATCH (a:A) REMOVE a:A")

    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.labels")

    // Then
    result shouldBe empty
  }

  test("db.labels should be empty when all nodes are removed") {
    // Given
    createLabeledNode("A")
    execute("MATCH (a) DETACH DELETE a")

    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.labels")

    // Then
    result shouldBe empty
  }

  test("db.labels should be empty even after db.createLabel") {
    // Given
    execute("CALL db.createLabel('A')")

    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.labels")

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
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.relationshipTypes")

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A"),
        Map("relationshipType" -> "B"),
        Map("relationshipType" -> "C")))
  }

  test("should get correct relationship types when added in transaction") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "B")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |CREATE ()-[:C]->()
        |WITH 1 as single
        |CALL db.relationshipTypes() YIELD relationshipType
        |RETURN relationshipType
      """.stripMargin)

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A"),
        Map("relationshipType" -> "B"),
        Map("relationshipType" -> "C")))
  }

  test("should get correct relationship types when removed in transaction") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH ()-[c:C]->()
        |DELETE c
        |WITH count(c) as single
        |CALL db.relationshipTypes() YIELD relationshipType
        |RETURN relationshipType
      """.stripMargin)

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A"),
        Map("relationshipType" -> "B")))
  }

  test("db.relationshipType work on an empty database") {
    // Given an empty database
    //When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.relationshipTypes")

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
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.relationshipTypes")

    // Then
    result shouldBe empty
  }

  test("should be able to find propertyKeys from built-in-procedure") {
    // Given
    createNode("A" -> 1, "B" -> 2, "C" -> 3)

    // When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.propertyKeys")

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
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.propertyKeys")

    // Then
    result shouldBe empty
  }

  test("removing properties from nodes and relationships does not remove them from the store") {
    // Given
    relate(createNode("A" -> 1), createNode("B" -> 1), "R" ->1)
    execute("MATCH (a)-[r]-(b) REMOVE a.A, r.R, b.B")

    // When
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.propertyKeys")

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
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.propertyKeys")

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
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.indexes")

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
      "Cannot yield value from void procedure.")
  }

  test("should create index from built-in-procedure") {
    // when
    val createResult = executeWith(Configs.ProcedureCallWrite, "CALL db.createIndex('MyIndex', ['Person'], ['name'], 'lucene+native-3.0')")

    // then
    createResult.toList should equal(
      List(Map(
        "name" -> "MyIndex",
        "labels" -> List("Person"),
        "properties" -> List("name"),
        "providerName" -> "lucene+native-3.0",
        "status" -> "index created"))
    )

    graph.withTx( tx => tx.execute("CALL db.awaitIndexes(10)"))

    val index = graph.withTx(tx => Iterators.single(tx.kernelTransaction().schemaRead().index(
      SchemaDescriptor.forLabel(tokenReader(tx, t => t.nodeLabel("Person")), tokenReader(tx, t => t.propertyKey("name"))))))
    // when
    val listResult = executeWith(Configs.ProcedureCallRead, "CALL db.indexes()")

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
    val createResult = executeWith(Configs.ProcedureCallWrite, "CALL db.createUniquePropertyConstraint('MyConstraint', ['Person'], ['name'],'lucene+native-3.0')")

    // then
    createResult.toList should equal(
      List(Map(
        "name" -> "MyConstraint",
        "labels" -> List("Person"),
        "properties" -> List("name"),
        "providerName" -> "lucene+native-3.0",
        "status" -> "uniqueness constraint online"))
    )

    graph.withTx( tx => tx.execute("CALL db.awaitIndexes(10)"))
    val index = inTx(tx => Iterators.single(tx.kernelTransaction().schemaRead().index(
      SchemaDescriptor.forLabel(tokenReader(tx, t => t.nodeLabel("Person")), tokenReader(tx, t => t.propertyKey("name"))))))

    // when
    val listResult = executeWith(Configs.ProcedureCallRead, "CALL db.indexes()")

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
    val createResult = executeWith(Configs.ProcedureCallWrite, "CALL db.createNodeKey('MyConstraint', ['Person'], ['name'],'lucene+native-3.0')")

    // then
    createResult.toList should equal(
      List(Map(
        "name" -> "MyConstraint",
        "labels" -> List("Person"),
        "properties" -> List("name"),
        "providerName" -> "lucene+native-3.0",
        "status" -> "node key constraint online"))
    )

    graph.withTx( tx => tx.execute("CALL db.awaitIndexes(10)"))
    val index = inTx(tx => Iterators.single(tx.kernelTransaction().schemaRead().index(
      SchemaDescriptor.forLabel(tokenReader(tx, t => t.nodeLabel("Person")), tokenReader(tx, t => t.propertyKey("name"))))))

    // when
    val listResult = executeWith(Configs.ProcedureCallRead, "CALL db.indexes()")

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
    val result = executeWith(Configs.ProcedureCallRead, "CALL db.indexes() YIELD name RETURN name")

    // Then
    result.columnAs("name").toList should equal(
      List("albert",
        "benny",
        "charlie",
        "poppy",
        "xavier"))
  }

  test("should be able to call kill query when there are open transactions without executing query") {
    //given, an empty transaction
    val transaction = graphOps.beginTx()

    try {
      //when
      val result = executeWith(Configs.ProcedureCallRead, "CALL dbms.killQuery('query-418218')")

      //then
      result.toList should equal(
        List(
          Map("queryId" -> "query-418218",
              "username" -> "n/a",
              "message" -> "No Query found with this id")))
    } finally {
      transaction.close()
    }
  }
}
