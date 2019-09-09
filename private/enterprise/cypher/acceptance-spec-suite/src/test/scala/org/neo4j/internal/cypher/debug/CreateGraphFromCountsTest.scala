/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.collector.DataCollectorMatchers._
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class CreateGraphFromCountsTest extends ExecutionEngineFunSuite with CypherComparisonSupport with CreateGraphFromCounts {

  test("should create uniqueness constraint") {
    val constraint = Constraint(Some("User"), None, Seq("name"), "Uniqueness constraint")
    val index = Index(Seq("User"), Seq("name"), 3, 2, 1)
    val json = data(constraint = Seq(constraint), index = Seq(index))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map("description" -> "CONSTRAINT ON ( user:User ) ASSERT (user.name) IS UNIQUE")
      )
    )

    executeSingle("CALL db.indexes").toList should beListInOrder(
      beMapContaining(
        "id" -> 1,
        "name" -> "index_1",
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
    val constraint = Constraint(Some("User"), None, Seq("name", "surname"), "Node key")
    val json = data(constraint = Seq(constraint))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map("description" -> "CONSTRAINT ON ( user:User ) ASSERT (user.name, user.surname) IS NODE KEY")
      )
    )
  }

  test("should create node existence constraint") {
    val constraint = Constraint(Some("User"), None, Seq("name"), "Existence constraint")
    val json = data(constraint = Seq(constraint))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map("description" -> "CONSTRAINT ON ( user:User ) ASSERT exists(user.name)")
      )
    )
  }

  test("should create relationship existence constraint") {
    val constraint = Constraint(None, Some("User"), Seq("name"), "Existence constraint")
    val json = data(constraint = Seq(constraint))

    createGraph(json)

    executeSingle("CALL db.constraints").toList should be(
      List(
        Map("description" -> "CONSTRAINT ON ()-[ user:User ]-() ASSERT exists(user.name)")
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
        "name" -> "index_1",
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
