/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.collector.DataCollectorMatchers.beListInOrder
import org.neo4j.internal.collector.DataCollectorMatchers.beMapContaining
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class CreateGraphFromCountsTest extends ExecutionEngineFunSuite with CypherComparisonSupport with CreateGraphFromCounts {

  test("should create uniqueness constraint") {
    val constraint = Constraint(Some("User"), None, Seq("name"), "Uniqueness constraint")
    val index = Index(Seq("User"), Seq("name"), 3, 2, 1)
    val json = data(constraint = Seq(constraint), index = Seq(index))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map(
          "name" -> "constraint_952bbd70",
          "description" -> "CONSTRAINT ON ( user:User ) ASSERT (user.name) IS UNIQUE",
          "details" -> "Constraint( id=2, name='constraint_952bbd70', type='UNIQUENESS', schema=(:User {name}), ownedIndex=1 )")
      )
    )

    executeSingle("CALL db.indexes").toList should beListInOrder(
      beMapContaining(
        "id" -> 1,
        "name" -> "constraint_952bbd70",
        // "state" -> // we don't care about state
        "uniqueness" -> "UNIQUE",
        // "populationPercent" -> // we don't care about specific population percentage,
        "properties" -> List("name"),
        "labelsOrTypes" -> List("User"),
        "entityType" -> "NODE",
        "type" -> "BTREE",
        "provider" -> "native-btree-1.0"
      )
    )
  }

  test("should create node key constraint, two properties") {
    val constraint = Constraint(Some("User"), None, Seq("name", "surname"), "Node Key")
    val json = data(constraint = Seq(constraint))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map(
          "name" -> "constraint_24564d05",
          "description" -> "CONSTRAINT ON ( user:User ) ASSERT (user.name, user.surname) IS NODE KEY",
          "details" -> "Constraint( id=2, name='constraint_24564d05', type='NODE KEY', schema=(:User {name, surname}), ownedIndex=1 )")
      )
    )
  }

  test("should create node existence constraint") {
    val constraint = Constraint(Some("User"), None, Seq("name"), "Existence constraint")
    val json = data(constraint = Seq(constraint))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map(
          "name" -> "constraint_89c8c370",
          "description" -> "CONSTRAINT ON ( user:User ) ASSERT (user.name) IS NOT NULL",
          "details" -> "Constraint( id=1, name='constraint_89c8c370', type='NODE PROPERTY EXISTENCE', schema=(:User {name}) )")
      )
    )
  }

  test("should create relationship existence constraint") {
    val constraint = Constraint(None, Some("User"), Seq("name"), "Existence constraint")
    val json = data(constraint = Seq(constraint))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map(
          "name" -> "constraint_36048d74",
          "description" -> "CONSTRAINT ON ()-[ user:User ]-() ASSERT (user.name) IS NOT NULL",
          "details" -> "Constraint( id=1, name='constraint_36048d74', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:User {name}]- )")
      )
    )
  }

  test("should create index") {
    val index = Index(Seq("Foo"), Seq("bar", "baz"), 3, 2, 1)
    val json = data(index = Seq(index))

    createGraph(json)

    executeSingle("CALL db.indexes").toList should beListInOrder(
      beMapContaining(
        "id" -> 1,
        "name" -> "index_ad031312",
        // "state" -> // we don't care about state
        "uniqueness" -> "NONUNIQUE",
        // "populationPercent" -> // we don't care about specific population percentage,
        "properties" -> List("bar", "baz"),
        "labelsOrTypes" -> List("Foo"),
        "entityType" -> "NODE",
        "type" -> "BTREE",
        "provider" -> "native-btree-1.0"
      )
    )
  }

  test("should create data with Node Label count") {
    val json = data(nodeCount = Seq(NodeCount(42, Some("Foo"))))

    createGraph(json)

    executeSingle("MATCH (n:Foo) RETURN n").toList should not be empty
  }

  test("should create data with Node count") {
    val json = data(nodeCount = Seq(NodeCount(42, None)))

    createGraph(json)

    executeSingle("MATCH (n) RETURN n").toList should not be empty
  }

  test("should create data with Node count and Node Label count") {
    val json = data(nodeCount = Seq(NodeCount(42, None), NodeCount(20, Some("Foo"))))

    createGraph(json)

    executeSingle("MATCH (n) RETURN n").toList should not be empty

    executeSingle("MATCH (n:Foo) RETURN n").toList should not be empty
  }

  test("should create relationships") {
    val json = data(nodeCount = Seq(NodeCount(10, Some("A")), NodeCount(20,Some("B")), NodeCount(10,Some( "C"))),
      relationshipCount = Seq(RelationshipCount(12, Some("R1"), Some("B"), None),
        RelationshipCount(8, Some("R1"), None, Some("A")), RelationshipCount(4, Some("R1"), None, Some("C"))))

    createGraph(json)

    executeSingle("MATCH (b:B)-[r:R1]->(a:A) RETURN count(r)").toList should be (List(Map("count(r)"-> 10)))
    executeSingle("MATCH (b:B)-[r:R1]->(c:C) RETURN count(r)").toList should be (List(Map("count(r)"-> 10)))
  }

  private def data(constraint: Seq[Constraint] = Seq.empty,
                   index: Seq[Index] = Seq.empty,
                   nodeCount: Seq[NodeCount] = Seq.empty,
                   relationshipCount: Seq[RelationshipCount] = Seq.empty): DbStatsRetrieveGraphCountsJSON = {
    DbStatsRetrieveGraphCountsJSON(Seq.empty,
      Seq(Result(Seq("section", "data"),
        Seq(Data(Row("GRAPH COUNTS",
          GraphCountData(
            constraint,
            index,
            nodeCount,
            relationshipCount
          )))))))
  }
}
