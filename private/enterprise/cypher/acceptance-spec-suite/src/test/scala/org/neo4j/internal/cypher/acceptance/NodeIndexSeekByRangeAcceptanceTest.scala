/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekByRange
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UniqueIndexSeekByRange
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.values.storable.DurationValue

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexSeekByRangeAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle comparing large integers") {
    // Given
    val person = createLabeledNode(Map("age" -> 5987523281782486379L), "Person")


    graph.createIndex("Person", "age")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (p:Person) USING INDEX p:Person(age) WHERE p.age > 5987523281782486378 RETURN p",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(Map("p" -> person)))
  }

  test("should handle comparing large integers 2") {
    // Given
    createLabeledNode(Map("age" -> 5987523281782486379L), "Person")


    graph.createIndex("Person", "age")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (p:Person) USING INDEX p:Person(age) WHERE p.age > 5987523281782486379 RETURN p",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result should be(empty)
  }

  test("should be case sensitive for STARTS WITH with indexes") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "london"), "Location")
    (1 to 100).foreach { _ =>
      createLabeledNode("Location")
    }
    (1 to 300).map { i =>
      createLabeledNode(Map("name" -> i.toString), "Location")
    }

    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name STARTS WITH 'Lon' RETURN l"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should perform prefix search in an update query") {
    createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "london"), "Location")
    for (i <- 1 to 100) createLabeledNode(Map("name" -> ("City" + i)), "Location")
    graph.createIndex("Location", "name")

    val query =
      """MATCH (l:Location) WHERE l.name STARTS WITH 'Lon'
        |CREATE (L:Location {name: toUpper(l.name)})
        |RETURN L.name AS NAME""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    result.toList should equal(List(Map("NAME" -> "LONDON")))
  }

  test("should only match on the actual prefix") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "Johannesburg"), "Location")
    createLabeledNode(Map("name" -> "Paris"), "Location")
    createLabeledNode(Map("name" -> "Malmo"), "Location")
    createLabeledNode(Map("name" -> "Loondon"), "Location")
    createLabeledNode(Map("name" -> "Lolndon"), "Location")

    (1 to 100).foreach { _ =>
      createLabeledNode("Location")
    }
    (1 to 300).map { i =>
      createLabeledNode(Map("name" -> i.toString), "Location")
    }
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name STARTS WITH 'Lon' RETURN l"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should plan the leaf with the longest prefix if multiple STARTS WITH patterns") {
    (1 to 100).foreach { _ =>
      createLabeledNode("Address")
    }
    (1 to 300).map { i =>
      createLabeledNode(Map("prop" -> i.toString), "Address")
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    // Add an uninteresting predicate using a parameter to stop autoparameterization from happening
    val result = executeWith(Configs.CachedProperty, """MATCH (a:Address)
                                                       |WHERE 43 = $apa
                                                       |  AND a.prop STARTS WITH 'w'
                                                       |  AND a.prop STARTS WITH 'www'
                                                       |RETURN a""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }), params = Map("apa" -> 43))

    result.toSet should equal(Set(Map("a" -> a1), Map("a" -> a2)))
    result.executionPlanDescription().toString should include("prop STARTS WITH \"www\"")
  }

  test("should plan an IndexRangeSeek for a STARTS WITH predicate search when index exists") {
    (1 to 100).foreach { _ =>
      createLabeledNode("Address")
    }
    (1 to 300).map { i =>
      createLabeledNode(Map("prop" -> i.toString), "Address")
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a:Address) WHERE a.prop STARTS WITH 'www' RETURN a",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    result.toSet should equal(Set(Map("a" -> a1), Map("a" -> a2)))
  }

  test("should plan a UniqueIndexSeek when constraint exists") {
    (1 to 100).foreach { _ =>
      createLabeledNode("Address")
    }
    (1 to 300).map { i =>
      createLabeledNode(Map("prop" -> i.toString), "Address")
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createUniqueConstraint("Address", "prop")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a:Address) WHERE a.prop STARTS WITH 'www' RETURN a",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(UniqueIndexSeekByRange.name)
      }))

    result.toSet should equal(Set(Map("a" -> a1), Map("a" -> a2)))
  }

  test("should be able to plan index seek for numerical less than") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> Double.NegativeInfinity),
      Map("prop" -> -5),
      Map("prop" -> 0),
      Map("prop" -> 5),
      Map("prop" -> 5.0)))
  }

  test("should be able to plan index seek for numerical less than (named index)") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndexWithName("prop_index", "Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> Double.NegativeInfinity),
      Map("prop" -> -5),
      Map("prop" -> 0),
      Map("prop" -> 5),
      Map("prop" -> 5.0)))
  }

  test("should be able to plan index seek for numerical negated greater than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop >= 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should
      equal(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0)
      ))
  }

  test("should be able to plan index seek for numerical less than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should
      equal(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 10),
        Map("prop" -> 10.0)
      ))
  }

  test("should be able to plan index seek for numerical negated greater than") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> Double.NegativeInfinity),
      Map("prop" -> -5),
      Map("prop" -> 0),
      Map("prop" -> 5),
      Map("prop" -> 5.0),
      Map("prop" -> 10),
      Map("prop" -> 10.0)
    ))
  }

  test("should be able to plan index seek for numerical greater than") {
    // Given matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > 5 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    // values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(10, 10.0, 100, Double.PositiveInfinity))
  }

  test("should be able to plan index seek for numerical negated less than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop <= 5 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    //values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(10, 10.0, 100, Double.PositiveInfinity))
  }

  test("should be able to plan index seek for numerical greater than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= 5 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    //values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(5, 5.0, 10, 10.0, 100, Double.PositiveInfinity))
  }

  test("should be able to plan index seek for numerical negated less than") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop < 5 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    // values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(5, 5.0, 10, 10.0, 100, Double.PositiveInfinity))
  }

  test("should be able to plan index seek for textual less than") {
    // Given matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> s"15${java.lang.Character.MIN_VALUE}"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < '15' RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.columnAs[String]("prop").toList should equal(Seq("", "-5", "0", "10", "14whatever"))
  }

  test("should be able to plan index seek for textual less than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> s"15${java.lang.Character.MIN_VALUE}"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= '15' RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.columnAs[String]("prop").toSet should equal(Set("", "-5", "0", "10", "15", "14whatever"))
  }

  test("should be able to plan index seek for textual greater than") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '15' RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.columnAs[String]("prop").toList should equal(Seq(smallValue, "5", "5"))
  }

  test("should be able to plan index seek for textual greater than or equal") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '15' RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.columnAs[String]("prop").toList should equal(Seq("15", smallValue, "5", "5"))
  }

  test("should be able to plan index seek without confusing property key ids") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given Non-matches
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop2" -> 5), "Label")
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop2" -> 10), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '15' AND n.prop2 > 5 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then

    result should be(empty)
  }

  test("should be able to plan index seek for empty numerical between range") {
    // Given
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= 10 AND n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result should be(empty)
  }

  test("should be able to plan index seek for numerical null range") {
    // Given
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= null RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result should be(empty)
  }

  test("should be able to plan index seek for non-empty numerical between range") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 6.1), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >=5 AND n.prop < 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> 5),
      Map("prop" -> 5.0),
      Map("prop" -> 6.1)
    ))
  }

  test("should be able to plan index seek using multiple non-overlapping numerical ranges") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 6.1), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 12.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= 0 AND n.prop >=5 AND n.prop < 10 AND n.prop < 100 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> 5),
      Map("prop" -> 5.0),
      Map("prop" -> 6.1)
    ))
  }

  test("should be able to plan index seek using multiple non-overlapping numerical ranges (named index)") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 6.1), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 12.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndexWithName("prop_index", "Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= 0 AND n.prop >=5 AND n.prop < 10 AND n.prop < 100 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> 5),
      Map("prop" -> 5.0),
      Map("prop" -> 6.1)
    ))
  }

  test("should be able to plan index seek using empty textual range") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < '15' AND n.prop >= '15' RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result should be(empty)
  }

  test("should be able to plan index seek using textual null range") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < null RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result should be(empty)
  }

  test("should be able to plan index seek using non-empty textual range") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '10' AND n.prop < '15' RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> "10"),
      Map("prop" -> "14whatever")
    ))
  }

  test("should be able to plan index seek using multiple non-overlapping textual ranges") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '1' AND n.prop >= '10' AND n.prop < '15' AND n.prop <= '14whatever' RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(
      Map("prop" -> "10"),
      Map("prop" -> "14whatever")
    ))
  }

  test("should be able to execute index seek using inequalities over different types as long as one inequality yields no results (1)") {
    // Given
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '1' AND n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result should be(empty)
  }

  test("should be able to execute index seek using inequalities over different types as long as one inequality yields no results (2)") {
    // Given
    createLabeledNode(Map("prop" -> 15), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '1' AND n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWith(Configs.CachedProperty, query)

    // Then
    result.columnAs[String]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include(IndexSeekByRange.name)
  }

  // TODO: re-enable linting for these queries where some predicates can be statically determined to always be false.
  ignore("should refuse to execute index seeks using inequalities over different types") {
    // Given
    createLabeledNode(Map("prop" -> 15), "Label")
    createLabeledNode(Map("prop" -> "1"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '1' AND n.prop > 10 RETURN n.prop AS prop"

    executeWith(Configs.CachedProperty, s"EXPLAIN $query",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    an[IllegalArgumentException] should be thrownBy {
      executeWith(Configs.Empty, query).toList
    }
  }

  test("should return no rows when executing index seeks using inequalities over incomparable types") {
    // Given
    (1 to 405).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= duration('P1Y1M') RETURN n.prop AS prop"

    val result = executeWith(Configs.UDF, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))
    result.toList should be(empty)
  }

  test("should return no rows when executing index seeks using inequalities over incomparable types but also comparing against null") {
    // Given
    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= $param AND n.prop < null RETURN n.prop AS prop"

    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }), params = Map("param" -> DurationValue.duration(1, 0, 0 ,0)))

    result should be(empty)
  }

  test("should plan range index seeks matching characters against properties (coerced to string wrt the inequality)") {
    // Given
    val nonMatchingChar = "X".charAt(0).charValue()
    val matchingChar = "Y".charAt(0).charValue()

    (1 to 500).foreach { _ =>
      createLabeledNode("Label")
    }
    createLabeledNode(Map("prop" -> matchingChar), "Label")
    createLabeledNode(Map("prop" -> matchingChar.toString), "Label")
    createLabeledNode(Map("prop" -> nonMatchingChar), "Label")
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= $param RETURN n.prop AS prop"

    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }), params = Map("param" -> matchingChar))

    result.toSet should equal(Set(Map("prop" -> matchingChar), Map("prop" -> matchingChar.toString)))
  }

  test("should plan range index seeks matching strings against character properties (coerced to string wrt the inequality)") {
    // Given
    val nonMatchingChar = "X".charAt(0).charValue()
    val matchingChar = "Y".charAt(0).charValue()

    (1 to 500).foreach { _ =>
      createLabeledNode("Label")
    }
    createLabeledNode(Map("prop" -> matchingChar), "Label")
    createLabeledNode(Map("prop" -> matchingChar.toString), "Label")
    createLabeledNode(Map("prop" -> nonMatchingChar), "Label")
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= $param RETURN n.prop AS prop"

    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }), params = Map("param" -> matchingChar.toString))

    result.toSet should equal(Set(Map("prop" -> matchingChar), Map("prop" -> matchingChar.toString)))
  }

  test("rule planner should plan index seek for inequality match") {
    graph.createIndex("Label", "prop")
    createLabeledNode(Map("prop" -> 1), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    for (_ <- 1 to 300) createLabeledNode("Label")

    val query = "MATCH (n:Label) WHERE n.prop < 10 CREATE () RETURN n.prop"

    val result = executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    result.toList should equal(List(Map("n.prop" -> 1), Map("n.prop" -> 5)))
  }

  test("should not use index seek by range when rhs of > inequality depends on property") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = "MATCH (a)-->(b:Label) WHERE b.prop > a.prop RETURN count(a) as c"
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan shouldNot includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(Map("c" -> size / 2)))
  }

  test("should use index seek by range when rhs of > inequality depends on variable in horizon") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = "WITH 200 AS x MATCH (b:Label) WHERE b.prop > x RETURN count(b) as c"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    // Then
    result.executionPlanDescription() should includeSomewhere.aPlan(IndexSeekByRange.name)
    result.toList should equal(List(Map("c" -> size / 2)))
  }

  test("should not use index seek by range when rhs of <= inequality depends on property") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = "MATCH (a)-->(b:Label) WHERE b.prop <= a.prop RETURN count(a) as c"
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan shouldNot includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(Map("c" -> size / 2)))
  }

  test("should not use index seek by range when rhs of >= inequality depends on same property") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = "MATCH (a)-->(b:Label) WHERE b.prop >= b.prop RETURN count(a) as c"
    val result = executeWith(Configs.CachedProperty, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan shouldNot includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(Map("c" -> size)))
  }

  test("should use index seek by range with literal on the lhs of inequality") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = s"MATCH (a)-->(b:Label) WHERE ${size / 2} < b.prop RETURN count(a) as c"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    assert(size > 20)
    result.toList should equal(List(Map("c" -> (size / 2))))
  }

  test("should use index seek by range with double inequalities") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = s"MATCH (a)-->(b:Label) WHERE 10 < b.prop <= ${size - 10} RETURN count(a) as c"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name)
      }))

    // Then
    assert(size > 20)
    result.toList should equal(List(Map("c" -> (size - 20))))
  }

  test("should use the index of inequality range scans") {
    (1 to 60).foreach { _ =>
      createLabeledNode(Map("gender" -> "male"), "Person")
    }
    (1 to 30).foreach { _ =>
      createLabeledNode(Map("gender" -> "female"), "Person")
    }
    (1 to 2).foreach { _ =>
      createLabeledNode("Person")
    }

    graph.createIndex("Person", "gender")

    graph.withTx( tx => {
      val result = tx.execute("CYPHER PROFILE MATCH (a:Person) WHERE a.gender > 'female' RETURN count(a) as c")

      result.asScala.toList.map(_.asScala) should equal(List(Map("c" -> 60)))
      result.getExecutionPlanDescription.toString should include(IndexSeekByRange.name)
      result.close()
    })
  }

  test("should use best index for range seek") {

    // Given
    (1 to 10).foreach { i =>
      createLabeledNode(Map("prop1" -> i), "L")
    }
    (1 to 45).foreach { i =>
      createLabeledNode(Map("prop2" -> i), "L")
      createLabeledNode(Map("prop2" -> i), "L")
    }

    // When
    graph.createIndex("L", "prop1")
    graph.createIndex("L", "prop2")
    Thread.sleep(1000)

    // Then
    executeWith(Configs.CachedProperty, "MATCH (n:L) WHERE n.prop2 > 1 AND n.prop1 > 1 RETURN n.prop1",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan(IndexSeekByRange.name).containingVariables("n").containingArgumentForCachedProperty("n", "prop1")
      }))
  }

  test("should not range seek index with 1 unique value (gt issue #12225)") {

    graph.withTx( tx => tx.execute(
      """UNWIND range(1, 10) AS i
        |CREATE (t:Tweet {created_date: 1})
        |WITH t, i
        |UNWIND range(1,10) AS j
        |CREATE (k:Keyword {value: i*10+j})
        |CREATE (t)-[:CONTAINS_KEYWORD]->(k)""".stripMargin))

    graph.createIndex("Tweet", "created_date")
    graph.createIndex("Keyword", "value")

    val query =
      s"""EXPLAIN
         |MATCH (k:Keyword)<-[r:CONTAINS_KEYWORD]-(t:Tweet)
         |WHERE k.value = 1234 AND t.created_date > 0
         |RETURN t, k""".stripMargin

    executeSingle(query).executionPlanDescription() should not(includeSomewhere.aPlan("NodeIndexSeekByRange"))
  }

  private def createTestModelBigEnoughToConsiderPickingIndexSeek: Int = {
    val size = 400

    (1 to size).foreach { i =>
      // Half of unlabeled nodes has prop = 0 (for even i:s prop = 0, for odd i:s prop = i)
      val a = createNode(Map("prop" -> (i & 1) * i))
      val b = createLabeledNode(Map("prop" -> i), "Label")
      relate(a, b)
    }

    // Create index _after_ nodes, so that when it comes online it is fully populated.
    graph.createIndex("Label", "prop")

    size
  }
}
