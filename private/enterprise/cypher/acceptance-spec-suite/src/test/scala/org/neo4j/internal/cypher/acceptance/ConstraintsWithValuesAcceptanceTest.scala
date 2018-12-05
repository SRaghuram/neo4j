/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

class ConstraintsWithValuesAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  override def beforeEach(): Unit = {
    super.beforeEach()
    createSomeNodes()
  }

  test("should use index when existence constraint for property") {
    // Given
    createSingleIndexes()
    graph.createExistenceConstraint("Awesome", "prop1")

    // Then
    assertIndexScan(standardResult, "Awesome")
  }

  test("should use index when existence constraint for multiple labels") {
    // Given
    createSingleIndexes()
    graph.execute("CREATE (:Awesome {prop1: 1337, prop2: 5})")
    graph.createExistenceConstraint("Awesome", "prop1")
    graph.createExistenceConstraint("Label", "prop1")

    // Then
    assertIndexScan(standardResult, "Label", "Awesome")
  }

  test("should use index when existence constraint for the first of multiple labels") {
    // Given
    createSingleIndexes()
    graph.execute("CREATE (:Awesome {prop1: 1337, prop2: 5})")
    graph.createExistenceConstraint("Label", "prop1")

    // Then
    assertIndexScan(standardResult, "Label")
  }

  test("should use index when existence constraint for the last of multiple labels") {
    // Given
    createSingleIndexes()
    graph.execute("CREATE (:Awesome {prop1: 1337, prop2: 5})")
    graph.createExistenceConstraint("Awesome", "prop1")

    // Then
    val expectedResult = List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44), Map("n.prop1" -> 1337))
    assertIndexScan(expectedResult, "Awesome")
  }

  test("should use index when existence constraint for multiple properties") {
    // Given
    createSingleIndexes()
    graph.createExistenceConstraint("Awesome", "prop1")
    graph.createExistenceConstraint("Awesome", "prop2")

    // Then
    assertIndexScan(standardResult, "Awesome")
  }

  test("should use index when node key constraint for property") {
    // Given
    graph.createNodeKeyConstraint("Awesome", "prop1")

    // Then
    assertIndexScan(standardResult, "Awesome")
  }

  test("should use single property index when composite node key constraint") {
    // Given
    createSingleIndexes()
    graph.createNodeKeyConstraint("Awesome", "prop1", "prop2")

    // Then
    assertIndexScan(standardResult, "Awesome")
  }

  test("index scan does not support composite index") {
    // Given
    graph.createNodeKeyConstraint("Awesome", "prop1", "prop2")

    // Then
    assertNodeByLabelScan(standardResult)
  }

  test("should not use index when no constraint") {
    // Given
    createSingleIndexes()

    // Then
    assertNodeByLabelScan(standardResult)
  }

  test("should not use index when unique constraint") {
    // Given
    graph.createUniqueConstraint("Awesome", "prop1")

    // Then
    assertNodeByLabelScan(standardResult)
  }

  test("should handle constraint drop") {
    // Given
    createSingleIndexes()
    graph.createExistenceConstraint("Awesome", "prop1")

    // Then
    assertIndexScan(standardResult, "Awesome")

    // When
    graph.execute("DROP CONSTRAINT ON (n:Awesome) ASSERT exists(n.prop1)")

    // Then
    assertNodeByLabelScan(standardResult)
  }

  test("should handle index drop") {
    // Given
    createSingleIndexes()
    graph.createExistenceConstraint("Awesome", "prop1")

    // Then
    assertIndexScan(standardResult, "Awesome")

    // When
    graph.execute("DROP INDEX ON :Awesome(prop1)")

    // Then
    assertNodeByLabelScan(standardResult)
  }

  test("existence constraint without index should give labelscan") {
    // Given
    graph.createExistenceConstraint("Awesome", "prop1")

    // Then
    assertNodeByLabelScan(standardResult)
  }

  private val standardResult = List(Map("n.prop1" -> 40), Map("n.prop1" -> 41), Map("n.prop1" -> 42), Map("n.prop1" -> 43), Map("n.prop1" -> 44))

  private def createSingleIndexes(): Unit = {
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Label", "prop1")
  }

  private def createSomeNodes(): Unit = {
    createLabeledNode(Map("prop1" -> 40, "prop2" -> 5), "Awesome", "Label")
    createLabeledNode(Map("prop1" -> 41, "prop2" -> 2), "Awesome", "Label")
    createLabeledNode(Map("prop1" -> 42, "prop2" -> 3), "Awesome", "Label")
    createLabeledNode(Map("prop1" -> 43, "prop2" -> 1), "Awesome", "Label")
    createLabeledNode(Map("prop1" -> 44, "prop2" -> 3), "Awesome", "Label")
  }

  private def assertNodeByLabelScan(expectedResult: Seq[Map[String, Int]]): Unit = {
    val query = "MATCH (n:Awesome) RETURN n.prop1"
    val result = executeWith(Configs.All, query)

    result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")
    result.toList should equal(expectedResult)
  }

  private def assertIndexScan(expectedResult: Seq[Map[String, Int]], labels: String*): Unit = {
    val query = s"MATCH (n:${labels.mkString(":")}) RETURN n.prop1"

    // NodeIndexScan is not supported in the compiled runtime
    val supportedConfig = Configs.InterpretedAndSlotted + Configs.Version3_5

    val result = executeWith(supportedConfig, query)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexScan")
          .withExactVariables("n", "cached[n.prop1]"))

    result.toList should equal(expectedResult)
  }
}
