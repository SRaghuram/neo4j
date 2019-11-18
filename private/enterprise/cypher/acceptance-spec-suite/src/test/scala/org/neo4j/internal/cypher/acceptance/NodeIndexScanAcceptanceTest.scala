/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{ComparePlansWithAssertion, Configs, CypherComparisonSupport}

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexScanAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport{

  test("should use index on IS NOT NULL") {
    // Given
    val person = createLabeledNode(Map("name" -> "Smith"), "Person")
    1 to 100 foreach (_ => createLabeledNode("Person"))
    graph.createIndex("Person", "name")

    // When
    val result = executeWith(Configs.CachedProperty,
      "MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p",
      planComparisonStrategy = ComparePlansWithAssertion( plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan")
      }))

    // Then
    result.toList should equal(List(Map("p" -> person)))
  }

  test("should use named index on IS NOT NULL") {
    // Given
    val person = createLabeledNode(Map("name" -> "Smith"), "Person")
    1 to 100 foreach (_ => createLabeledNode("Person"))
    graph.createIndexWithName("name_index", "Person", "name")

    // When
    val result = executeWith(Configs.CachedProperty,
      "MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p",
      planComparisonStrategy = ComparePlansWithAssertion( plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan")
      }))

    // Then
    result.toList should equal(List(Map("p" -> person)))
  }

  test("should use index on exists") {
    // Given
    val person = createLabeledNode(Map("name" -> "Smith"), "Person")
    1 to 100 foreach (_ => createLabeledNode("Person"))
    graph.createIndex("Person", "name")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
                             "MATCH (p:Person) WHERE exists(p.name) RETURN p",
                             planComparisonStrategy = ComparePlansWithAssertion( plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan")
      }))

    // Then
    result.toList should equal(List(Map("p" -> person)))
  }

  test("Regexp filter on top of NodeIndexScan (GH #7059)") {
    // Given
    graph.createIndex("phone_type", "label")
    createLabeledNode(Map("id" -> "8bbee2f14a493fa08bef918eaac0c57caa4f9799", "label" -> "ay"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "5fe613811746aa2a1c29d04c6e107974c3a92486", "label" -> "aa"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "a573480b0f13ca9f33d82df91392f9397031a687", "label" -> "g"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "cf8d30601edd84e19f45e9dfd18d2342b92a36eb", "label" -> "t"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "dd4121287c9b542269bda97744d9e828a46bdac4", "label" -> "ae"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "cf406352f4436a5399025fa3f7a7336a24dabdd3", "label" -> "uw"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "a7d330b126594cd47000193698c8d0f9650bc8c4", "label" -> "eh"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "62021e8bd919b0e84427a1e08dfd7704e6a6bd88", "label" -> "r"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "ea1e1deb3f3634ba823a2d5dac56f8bbd2b5b66d", "label" -> "k"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "35321f07bc9b3028d3f5ee07808969a3bd7d76ee", "label" -> "s"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "c960cff2a19a2088c5885f82725428db0694d7ae", "label" -> "d"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "139dbf46f0dc8a325e27ffd118331ca2947e34f0", "label" -> "z"), "phone_type", "timed")

    // When
    val result = executeWith(Configs.CachedProperty,
      "MATCH (n:phone_type:timed) where n.label =~ 'a.' return count(n)",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan")
      }))

    // Then
    result.toList should equal(List(Map("count(n)" -> 3)))
  }

  test("should work just fine and use an index scan") {
    graph.createIndex("Method", "arg0")
    val query =
      """
        |match (f:XMLElement:Function)<-[r:Use]-
        | (p:XMLElement:Product)-[:ReferTo]->
        | (pc:Class)-[:Declares]->
        | (pm:Method)
        | WHERE pm.arg0 = r.name
        | merge (pm)-[:Call]->(f);
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    result.toList should be(empty)
  }

  test("should not union identical NodeIndexScans") {
    val as = (1 to 209).map(_ => createLabeledNode("A"))
    val cs = (1 to 12).map(_ => createLabeledNode("C"))
    (1 to 2262).map(i => createLabeledNode("Other"))
    as.zipWithIndex.foreach { case(aNode, i) => relate(cs(i % cs.size), aNode) }

    // Empty index so that plan with NodeIndexScan becomes very cheap
    graph.createIndex("A", "id")

    val q =
      """
        |MATCH (c:C)-[:REL]->(a:A)
        |MATCH (c)-[:REL]->(b:A)
        |  WHERE a.id = b.id OR a.id < b.id
        |RETURN *
      """.stripMargin

    val result = executeWith(Configs.CachedProperty, q)
    result.executionPlanDescription() should (not(includeSomewhere.aPlan("Union")
      .withLHS(aPlan("NodeIndexScan"))
      .withRHS(aPlan("NodeIndexScan"))
    ) and includeSomewhere.aPlan("NodeIndexScan"))

    result should be(empty)
  }

  test("should not union identical NodeIndexScans in a non CNF normalized predicate") {
    val as = (1 to 209).map(_ => createLabeledNode("A"))
    val cs = (1 to 12).map(_ => createLabeledNode("C"))
    (1 to 2262).map(i => createLabeledNode("Other"))
    as.zipWithIndex.foreach { case(aNode, i) => relate(cs(i % cs.size), aNode, ("id", i)) }

    // Empty index so that plan with NodeIndexScan becomes very cheap
    graph.createIndex("A", "id")

    val q =
      """
        |MATCH (c:C)-[r1:REL]->(a:A)
        |MATCH (c)-[r2:REL]->(b:A)
        |  WHERE (a.id = b.id AND r1.id < r2.id) OR a.id < b.id
        |RETURN *
      """.stripMargin

    val result = executeWith(Configs.CachedProperty, q)
    result.executionPlanDescription() should (not(includeSomewhere.aPlan("Union")
      .withLHS(aPlan("NodeIndexScan"))
      .withRHS(aPlan("NodeIndexScan"))
    ) and includeSomewhere.aPlan("NodeIndexScan"))

    result should be(empty)
  }
}
