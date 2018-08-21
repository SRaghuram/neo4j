/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexSeekAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport{

  test("should handle OR when using index") {
    // Given
    graph.createIndex("L", "prop")
    val node1 = createLabeledNode(Map("prop" -> 1), "L")
    val node2 = createLabeledNode(Map("prop" -> 2), "L")
    createLabeledNode(Map("prop" -> 3), "L")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:L) WHERE n.prop = 1 OR n.prop = 2 RETURN n")

    // Then
    result should useOperationTimes("NodeIndexSeek", 1)
    result.toList should equal(List(Map("n" -> node1), Map("n" -> node2)))
  }

  test("should handle AND when using index") {
    // Given
    graph.createIndex("L", "prop")
    createLabeledNode(Map("prop" -> 1), "L")
    createLabeledNode(Map("prop" -> 2), "L")
    createLabeledNode(Map("prop" -> 3), "L")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:L) WHERE n.prop = 1 AND n.prop = 2 RETURN n")

    // Then
    result should useOperationTimes("NodeIndexSeek", 1)
    result.toList shouldBe empty
  }

  test("Should allow AND and OR with index and equality predicates") {
    graph.createIndex("User", "prop1")
    graph.createIndex("User", "prop2")
    val nodes = Range(0, 100).map(i => createLabeledNode(Map("prop1" -> i, "prop2" -> i), "User"))

    val query =
      """MATCH (c:User)
        |WHERE ((c.prop1 = 1 AND c.prop2 = 1)
        |OR (c.prop1 = 11 AND c.prop2 = 11))
        |RETURN c""".stripMargin

    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.columnAs("c").toSet should be(Set(nodes(1), nodes(11)))
    result should useOperationTimes("NodeIndexSeek", 2)
    result should use("Union")
  }

  test("Should allow AND and OR with index and inequality predicates") {
    graph.createIndex("User", "prop1")
    graph.createIndex("User", "prop2")
    val nodes = Range(0, 100).map(i => createLabeledNode(Map("prop1" -> i, "prop2" -> i), "User"))

    val query =
      """MATCH (c:User)
        |WHERE ((c.prop1 >= 1 AND c.prop2 < 2)
        |OR (c.prop1 > 10 AND c.prop2 <= 11))
        |RETURN c""".stripMargin

    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)
    result.columnAs("c").toSet should be(Set(nodes(1), nodes(11)))
    result should useOperationTimes("NodeIndexScan", 2)
    result should use("Union")
  }

  test("Should allow AND and OR with index seek and STARTS WITH predicates") {
    graph.createIndex("User", "prop1")
    graph.createIndex("User", "prop2")
    val nodes = Range(0, 100).map(i => createLabeledNode(Map("prop1" -> s"${i}_val", "prop2" -> s"${i}_val"), "User"))

    val query =
      """MATCH (c:User)
        |WHERE ((c.prop1 STARTS WITH '1_' AND c.prop2 STARTS WITH '1_')
        |OR (c.prop1 STARTS WITH '11_' AND c.prop2 STARTS WITH '11_'))
        |RETURN c""".stripMargin

    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.columnAs("c").toSet should be(Set(nodes(1), nodes(11)))
    result should useOperationTimes("NodeIndexSeekByRange", 2)
    result should use("Union")
  }

  test("Should allow AND and OR with index scan and regex predicates") {
    graph.createIndex("User", "prop1")
    graph.createIndex("User", "prop2")
    val nodes = Range(0, 100).map(i => createLabeledNode(Map("prop1" -> s"${i}_val", "prop2" -> s"${i}_val"), "User"))

    val query =
      """MATCH (c:User)
        |WHERE ((c.prop1 =~ '1_.*' AND c.prop2 =~ '1_.*')
        |OR (c.prop1 =~ '11_.*' AND c.prop2 =~ '11_.*'))
        |RETURN c""".stripMargin

    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.columnAs("c").toSet should be(Set(nodes(1), nodes(11)))
    result should useOperationTimes("NodeIndexScan", 2)
    result should use("Union")
  }

  test("Should allow OR with index scan and regex predicates") {
    graph.createIndex("User", "prop")
    val nodes = Range(0, 100).map(i => createLabeledNode(Map("prop" -> s"${i}_val"), "User"))

    val query =
      """MATCH (c:User)
        |WHERE c.prop =~ '1_.*' OR c.prop =~ '11_.*'
        |RETURN c""".stripMargin

    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.columnAs("c").toSet should be(Set(nodes(1), nodes(11)))
    result should useOperationTimes("NodeIndexScan", 2)
    result should use("Union")
  }

  test("should not forget predicates") {
    setUpDatabaseForTests()

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Crew) WHERE n.name = 'Neo' AND n.name = 'Morpheus' RETURN n")

    // Then
    result should (use("NodeIndexSeek") and be(empty))
  }


  test("should be able to use value coming from UNWIND for index seek") {
    // Given
    graph.createIndex("Prop", "id")
    val n1 = createLabeledNode(Map("id" -> 1), "Prop")
    val n2 = createLabeledNode(Map("id" -> 2), "Prop")
    val n3 = createLabeledNode(Map("id" -> 3), "Prop")
    for (i <- 4 to 30) createLabeledNode(Map("id" -> i), "Prop")

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("unwind [1,2,3] as x match (n:Prop) where n.id = x return n;")

    // Then
    val expected = List(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3))
    result should (use("NodeIndexSeek") and evaluateTo(expected))
  }

  test("should use index selectivity when planning") {
    // Given
    graph.inTx{
      val ls = (1 to 100).map { i =>
        createLabeledNode(Map("l" -> i), "L")
      }

      val rs = (1 to 100).map { i =>
        createLabeledNode(Map("r" -> 23), "R")
      }

      for (l <- ls ; r <- rs) {
        relate(l, r, "REL")
      }
    }

    // note: creating index after the nodes makes sure that we have statistics when the indexes come online
    graph.createIndex("L", "l")
    graph.createIndex("R", "r")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (l:L {l: 9})-[:REL]->(r:R {r: 23}) RETURN l, r")
    result should (use("NodeIndexSeek") and have size 100)
  }

  test("should handle nulls in index lookup") {
    // Given
    val cat = createLabeledNode("Cat")
    val dog = createLabeledNode("Dog")
    relate(cat, dog, "FRIEND_OF")

    // create many nodes with label 'Place' to make sure index seek is planned
    (1 to 100).foreach(i => createLabeledNode(Map("name" -> s"Area $i"), "Place"))

    graph.createIndex("Place", "name")

    // When
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(
      """
        |MATCH ()-[f:FRIEND_OF]->()
        |WITH f.placeName AS placeName
        |OPTIONAL MATCH (p:Place)
        |WHERE p.name = placeName
        |RETURN p, placeName
      """.stripMargin)

    // Then
    result should (use("NodeIndexSeek") and evaluateTo(List(Map("p" -> null, "placeName" -> null))))
  }

  test("should not use indexes when RHS of property comparison depends on the node searched for (equality)") {
    // Given
    val n1 = createLabeledNode(Map("a" -> 1), "MyNodes")
    val n2 = createLabeledNode(Map("a" -> 0), "MyNodes")
    val n3 = createLabeledNode(Map("a" -> 1, "b" -> 1), "MyNodes")
    val n4 = createLabeledNode(Map("a" -> 1, "b" -> 5), "MyNodes")

    graph.createIndex("MyNodes", "a")

    val query =
      """|MATCH (m:MyNodes)
        |WHERE m.a = coalesce(m.b, 0)
        |RETURN m""".stripMargin

    // When
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    // Then
    result.toList should equal(List(
      Map("m" -> n2),
      Map("m" -> n3)
    ))
    result.executionPlanDescription().toString shouldNot include("Index")
  }

  test("should not use indexes when RHS of property comparison depends on the node searched for (range query)") {
    // Given
    val n1 = createLabeledNode(Map("a" -> 1), "MyNodes")
    val n2 = createLabeledNode(Map("a" -> 0), "MyNodes")
    val n3 = createLabeledNode(Map("a" -> 1, "b" -> 1), "MyNodes")
    val n4 = createLabeledNode(Map("a" -> 5, "b" -> 1), "MyNodes")

    graph.createIndex("MyNodes", "a")

    val query =
      """|MATCH (m:MyNodes)
        |WHERE m.a > coalesce(m.b, 0)
        |RETURN m""".stripMargin

    // When
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    // Then
    result.toList should equal(List(
      Map("m" -> n1),
      Map("m" -> n4)
    ))
    result.executionPlanDescription().toString shouldNot include("Index")
  }

  test("should handle array as parameter when using index") {
    // Given
    graph.createIndex("Company", "uuid")
    val root1 = createLabeledNode(Map("uuid" -> "b"), "Company")
    val root2 = createLabeledNode(Map("uuid" -> "a"), "Company")
    val root3 = createLabeledNode(Map("uuid" -> "c"), "Company")
    createLabeledNode(Map("uuid" -> "z"), "Company")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      "MATCH (root:Company) WHERE root.uuid IN {uuids} RETURN DISTINCT root",
      "uuids" -> Array("a", "b", "c"))

    //Then
    result should useOperationTimes("NodeIndexSeek", 1)
    result.toList should contain theSameElementsAs List(Map("root" -> root1), Map("root" -> root2), Map("root" -> root3))
  }

  test("should handle primitive array as parameter when using index") {
    // Given
    graph.createIndex("Company", "uuid")
    val root1 = createLabeledNode(Map("uuid" -> 1), "Company")
    val root2 = createLabeledNode(Map("uuid" -> 2), "Company")
    val root3 = createLabeledNode(Map("uuid" -> 3), "Company")
    createLabeledNode(Map("uuid" -> 6), "Company")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      "MATCH (root:Company) WHERE root.uuid IN {uuids} RETURN DISTINCT root",
      "uuids" -> Array(1, 2, 3))

    //Then
    result should useOperationTimes("NodeIndexSeek", 1)
    result.toList should contain theSameElementsAs List(Map("root" -> root1), Map("root" -> root2), Map("root" -> root3))
  }

  test("should handle list properties in index") {
    // Given
    graph.createIndex("L", "prop")
    val node1 = createLabeledNode(Map("prop" -> Array(1,2,3)), "L")
    val node2 = createLabeledNode(Map("prop" -> Array(3,2,1)), "L")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:L) WHERE n.prop = [1,2,3] RETURN n")

    // Then
    result.toList should equal(List(Map("n" -> node1)))
    result should useOperationTimes("NodeIndexSeek", 1)
  }

  test("should handle list properties in unique index") {
    // Given
    graph.createConstraint("L", "prop")
    val node1 = createLabeledNode(Map("prop" -> Array(1,2,3)), "L")
    val node2 = createLabeledNode(Map("prop" -> Array(3,2,1)), "L")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:L) WHERE n.prop = [1,2,3] RETURN n")

    // Then
    result.toList should equal(List(Map("n" -> node1)))
    result should useOperationTimes("NodeUniqueIndexSeek", 1)
  }

  test("should not return any rows for OR predicates with different labels gh#12017") {
    // Given
    graph.createIndex("Label1", "prop1")
    graph.createIndex("Label2", "prop2")
    graph.execute("CREATE(:Label1 {prop1: 'val'})" )

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Label1:Label2) WHERE n.prop1 = 'val' OR n.prop2 = 'val' RETURN n")

    // Then
    result.toList should be (empty)
    result should useOperationTimes("NodeIndexSeek", 2)
  }

  test("should be able to solve OR predicates with same label") {
    // Given
    graph.createIndex("Label1", "prop1")
    graph.createIndex("Label1", "prop2")
    val node1 = createLabeledNode(Map("prop1" -> "val"), "Label1")
    val node2 = createLabeledNode(Map("prop2" -> "anotherVal"), "Label1")

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Label1) WHERE n.prop1 = 'val' OR n.prop2 = 'val' RETURN n")

    // Then
    result.toList should equal(List(Map("n" -> node1)))
    result should useOperationTimes("NodeIndexSeek", 2)
  }

  test("should not return any rows for OR predicates with four indexes") {
    // Given
    graph.createIndex("Label1", "prop1")
    graph.createIndex("Label1", "prop2")
    graph.createIndex("Label2", "prop1")
    graph.createIndex("Label2", "prop2")

    for( i <- 1 to 10 ) {
      graph.execute("CREATE(:Label1 {prop1: 'val', prop2: 'val'})" )
      graph.execute("CREATE(:Label2 {prop1: 'val', prop2: 'val'})" )
    }

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Label1:Label2) WHERE n.prop1 = 'val' OR n.prop2 = 'val' RETURN n")

    // Then
    result.toList should be (empty)
    result should useOperationTimes("NodeIndexSeek", 4)
  }

  private def setUpDatabaseForTests() {
    updateWithBothPlannersAndCompatibilityMode(
      """CREATE (architect:Matrix { name:'The Architect' }),
        |       (smith:Matrix { name:'Agent Smith' }),
        |       (cypher:Matrix:Crew { name:'Cypher' }),
        |       (trinity:Crew { name:'Trinity' }),
        |       (morpheus:Crew { name:'Morpheus' }),
        |       (neo:Crew { name:'Neo' }),
        |       (smith)-[:CODED_BY]->(architect),
        |       (cypher)-[:KNOWS]->(smith),
        |       (morpheus)-[:KNOWS]->(trinity),
        |       (morpheus)-[:KNOWS]->(cypher),
        |       (neo)-[:KNOWS]->(morpheus),
        |       (neo)-[:LOVES]->(trinity)""".stripMargin)

    for (i <- 1 to 10) createLabeledNode(Map("name" -> ("Joe" + i)), "Crew")

    for (i <- 1 to 10) createLabeledNode(Map("name" -> ("Smith" + i)), "Matrix")

    graph.createIndex("Crew", "name")
  }
}
