/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v4_0.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexEndsWithScanAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport{
  test("should be case sensitive for ENDS WITH with indexes") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name ENDS WITH 'ondon' RETURN l"

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexEndsWithScan")))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should be case sensitive for ENDS WITH with unique indexes") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createUniqueConstraint("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name ENDS WITH 'ondon' RETURN l"

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexEndsWithScan")))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should be case sensitive for ENDS WITH with multiple indexes and predicates") {
    val london = createLabeledNode(Map("name" -> "London", "country" -> "UK"), "Location")
    createLabeledNode(Map("name" -> "LONDON", "country" -> "UK"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString, "country" -> "UK"), "Location")
      }
    }

    graph.createIndex("Location", "name")
    graph.createIndex("Location", "country")

    val query = "MATCH (l:Location) WHERE l.name ENDS WITH 'ondon' AND l.country = 'UK' RETURN l"

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexEndsWithScan")))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should not use endsWith index scan with multiple indexes and predicates where other index is more selective") {
    val london = createLabeledNode(Map("name" -> "London", "country" -> "UK"), "Location")
    createLabeledNode(Map("name" -> "LONDON", "country" -> "UK"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createIndex("Location", "name")
    graph.createIndex("Location", "country")

    val query = "MATCH (l:Location) WHERE l.name ENDS WITH 'ondon' AND l.country = 'UK' RETURN l"

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeek")))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should use endsWith index with multiple indexes and predicates where other index is more selective but we add index hint") {
    val london = createLabeledNode(Map("name" -> "London", "country" -> "UK"), "Location")
    createLabeledNode(Map("name" -> "LONDON", "country" -> "UK"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createIndex("Location", "name")
    graph.createIndex("Location", "country")

    val query = "MATCH (l:Location) USING INDEX l:Location(name) WHERE l.name ENDS WITH 'ondon' AND l.country = 'UK' RETURN l"

    // RULE has bug with this query
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexEndsWithScan")))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should return nothing when invoked with a null value") {
    createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createUniqueConstraint("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name ENDS WITH {param} RETURN l"

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexEndsWithScan")),
      params = Map("param" -> null))

    result.toList should equal(List.empty)
  }

  test("throws appropriate type error") {
    createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createUniqueConstraint("Location", "name")

    val config = Configs.InterpretedAndSlottedAndMorsel
    val query = "MATCH (l:Location) WHERE l.name ENDS WITH {param} RETURN l"
    val message = List("Expected a string value, but got 42","Expected a string value, but got Int(42)","Expected two strings, but got London and 42")

    failWithError(config, query, message, params = Map("param" -> 42))
  }
}
