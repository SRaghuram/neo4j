/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ConstraintsWithValuesAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport  {

  override def beforeEach(): Unit = {
    super.beforeEach()
    createSomeNodes()
  }

  private def createSingleIndexes(): Unit = {
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Label", "prop1")
  }

  // Invoked once before the Tx and once in the same Tx
  def createSomeNodes(): Unit = {
    graph.execute(
      """
      CREATE (:Awesome:Label {prop1: 40, prop2: 5})-[:R]->()
      CREATE (:Awesome:Label {prop1: 41, prop2: 2})-[:R]->()
      CREATE (:Awesome:Label {prop1: 42, prop2: 3})-[:R]->()
      CREATE (:Awesome:Label {prop1: 43, prop2: 1})-[:R]->()
      CREATE (:Awesome:Label {prop1: 44, prop2: 3})-[:R]->()
      """)
  }

  test("should use index when existence constraint for property") {
    // Given
    createSingleIndexes()
    graph.createExistenceConstraint("Awesome", "prop1")

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexScan")
          .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("should use index when existence constraint for property - multiple labels") {
    // Given
    createSingleIndexes()
    graph.execute(
      """
        |CREATE (:Awesome {prop1: 1337, prop2: 5})
      """.stripMargin)
    graph.createExistenceConstraint("Awesome", "prop1")
    graph.createExistenceConstraint("Label", "prop1")

    // When
    val query = "MATCH (n:Label:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexScan")
          .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("should use index when existence constraint for multiple properties") {
    // Given
    createSingleIndexes()
    graph.createExistenceConstraint("Awesome", "prop1")
    graph.createExistenceConstraint("Awesome", "prop2")

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexScan"))
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("should use index when node key constraint for property") {
    // Given
    graph.createNodeKeyConstraint("Awesome", "prop1")

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexScan")
          .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("should not use index when no constraint") {
    // Given
    createSingleIndexes()

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("should not use index when unique constraint") {
    // Given
    graph.createUniqueConstraint( "Awesome", "prop1")

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("no support for using index when composite node key constraint") {
    // Given
    graph.createNodeKeyConstraint("Awesome", "prop1", "prop2")

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("no support for using composite index") {
    // Given
    graph.createIndex("Awesome", "prop1", "prop2")
    graph.createExistenceConstraint("Awesome", "prop1")

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

  test("existence constraint without index should give labelscan")
  {
    // Given
    graph.createExistenceConstraint("Awesome", "prop1")

    // When
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeSingle(query)

    // Then
    result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")
    result.toList should equal(List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44)))
  }

}
