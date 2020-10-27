/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class MergeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport
                          with CypherComparisonSupport {

  test("multiple merges after each other") {
    1 to 100 foreach { prop =>
      val result = executeWith(Configs.InterpretedAndSlotted, s"merge (a:Label {prop: $prop}) return a.prop")
      assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
    }
  }

  test("should not accidentally create relationship between wrong nodes after merge") {
    // Given
    val query =
      """
        |MERGE (a:A)
        |MERGE (b:B)
        |MERGE (c:C)
        |WITH a, b, c
        |CREATE (b)-[r:R]->(c)
        |RETURN r
      """.stripMargin

    // When
    val r = graph.withTx( tx => {
      val result = tx.execute(s"CYPHER runtime=slotted $query")

      // Then
      val row = result.next
      row.get("r").asInstanceOf[Relationship]
    })

    graph.withTx( tx => {
      val relationship = tx.getRelationshipById(r.getId)
      val labelB = relationship.getStartNode.getLabels.iterator().next()
      val labelC = relationship.getEndNode.getLabels.iterator().next()
      labelB.name() shouldEqual "B"
      labelC.name() shouldEqual "C"
    } )
  }

  test("Merging with self loop and relationship uniqueness") {
    graph.withTx( tx => tx.execute("CREATE (a) CREATE (a)-[:X]->(a)"))
    val result = executeWith(Configs.InterpretedAndSlotted, "MERGE (a)-[:X]->(b)-[:X]->(c) RETURN 42")
    assertStats(result, relationshipsCreated = 2, nodesCreated = 3)
  }

  test("Merging with self loop and relationship uniqueness - no stats") {
    graph.withTx( tx => tx.execute("CREATE (a) CREATE (a)-[:X]->(a)"))
    val result = executeWith(Configs.InterpretedAndSlotted, "MERGE (a)-[r1:X]->(b)-[r2:X]->(c) RETURN id(r1) = id(r2) as sameEdge")
    result.columnAs[Boolean]("sameEdge").toList should equal(List(false))
  }

  test("Merging with self loop and relationship uniqueness - no stats - reverse direction") {
    graph.withTx( tx => tx.execute("CREATE (a) CREATE (a)-[:X]->(a)"))
    val result = executeWith(Configs.InterpretedAndSlotted, "MERGE (a)-[r1:X]->(b)<-[r2:X]-(c) RETURN id(r1) = id(r2) as sameEdge")
    result.columnAs[Boolean]("sameEdge").toList should equal(List(false))
  }

  test("Merging with non-self-loop but require relationship uniqueness") {
    val a = createLabeledNode(Map("name" -> "a"), "A")
    val b = createLabeledNode(Map("name" -> "b"), "B")
    relate(a, b, "X")
    val result = executeWith(Configs.InterpretedAndSlotted, "MERGE (a)-[r1:X]->(b)<-[r2:X]-(c) RETURN id(r1) = id(r2) as sameEdge, c.name as name")
    result.toList should equal(List(Map("sameEdge" -> false, "name" -> null)))
  }

  test(s"correct error message on null MERGE") {
    failWithError(Configs.InterpretedAndSlotted, "MERGE (x:Order {OrderId:null})",
      Seq("Cannot merge the following node because of null property value for 'OrderId': (x:Order {OrderId: null})",
        "Cannot merge the following node because of null property value for 'OrderId': (:Order {OrderId: null})"))
    // we accept with and without variable names because interpreted knows the variable, while slotted does not
  }

  test(s"correct error message on null MERGE should not expose internally generated variable names") {
    failWithError(Configs.InterpretedAndSlotted, "MERGE (:Order {OrderId:null})",
      "Cannot merge the following node because of null property value for 'OrderId': (:Order {OrderId: null})")
  }

  test(s"correct error message on null MERGE with any amount of labels should work") {
    failWithError(Configs.InterpretedAndSlotted, "MERGE (:Order:Priority {OrderId:null})",
      "Cannot merge the following node because of null property value for 'OrderId': (:Order:Priority {OrderId: null})")
    failWithError(Configs.InterpretedAndSlotted, "MERGE ({OrderId:null})",
      "Cannot merge the following node because of null property value for 'OrderId': ( {OrderId: null})")
  }

  test(s"correct error message on null MERGE on relationship") {
    failWithError(Configs.InterpretedAndSlotted, "MERGE (y:Order)-[x:ORDERED_BY {date:null}]->(z:Customer)",
      Seq("Cannot merge the following relationship because of null property value for 'date': (y)-[x:ORDERED_BY {date: null}]->(z)",
        "Cannot merge the following relationship because of null property value for 'date': (y)-[:ORDERED_BY {date: null}]->(z)"))
      // we accept with and without variable name because interpreted knows the variable, while slotted does not
  }

  test(s"correct error message on null MERGE on relationship should not expose internally generated variable names") {
    failWithError(Configs.InterpretedAndSlotted, "MERGE (:Order)-[:ORDERED_BY {date:null}]->(:Customer)",
      "Cannot merge the following relationship because of null property value for 'date': ()-[:ORDERED_BY {date: null}]->()")
    failWithError(Configs.InterpretedAndSlotted, "MERGE (x:Order)-[:ORDERED_BY {date:null}]->(y:Customer)",
      "Cannot merge the following relationship because of null property value for 'date': (x)-[:ORDERED_BY {date: null}]->(y)")
    failWithError(Configs.InterpretedAndSlotted, "MERGE (x:Order)-[:ORDERED_BY {date:null}]->(:Customer)",
      "Cannot merge the following relationship because of null property value for 'date': (x)-[:ORDERED_BY {date: null}]->()")
    failWithError(Configs.InterpretedAndSlotted, "MERGE (:Order)-[:ORDERED_BY {date:null}]->(z:Customer)",
      "Cannot merge the following relationship because of null property value for 'date': ()-[:ORDERED_BY {date: null}]->(z)")
    failWithError(Configs.InterpretedAndSlotted, "MERGE (:Order)-[x:ORDERED_BY {date:null}]->(:Customer)",
      Seq("Cannot merge the following relationship because of null property value for 'date': ()-[x:ORDERED_BY {date: null}]->()",
        "Cannot merge the following relationship because of null property value for 'date': ()-[:ORDERED_BY {date: null}]->()"))
    // we accept with and without variable name because interpreted knows the variable, while slotted does not
  }

  test("should give sensible error message on add relationship to null node") {
    val query =
      """OPTIONAL MATCH (a)
        |MERGE (a)-[r:X]->()
      """.stripMargin

    failWithError(Configs.InterpretedAndSlotted, query,
      "Failed to create relationship `r`, node `a` is missing. " +
        "If you prefer to simply ignore rows where a relationship node is missing, " +
        "set 'cypher.lenient_create_relationship = true' in neo4j.conf")
  }

  test("should handle cached node properties with merge relationship") {
    graph.createUniqueIndex("User", "surname")
    val n1 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Smoke"), "User")
    val n2 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    relate(n2, n1, "Knows")

    val q =
      """
        |MATCH (n:User)
        |WHERE n.surname = 'Soap'
        |MERGE (n)-[r:Knows]->(s:User {surname: 'Smoke'})
        |RETURN s.surname AS name
      """.stripMargin

    val r = executeWith(Configs.InterpretedAndSlotted, q)
    r.toList should equal(List(Map("name" -> "Smoke")))
  }

  test("should not choke on cached properties followed by distinct and properties set") {
    // Given
    graph.createIndex("L", "prop")
    createLabeledNode(Map("prop" -> 42, "other" -> 43), "L")

    // When
    val result = executeWith(Configs.InterpretedAndSlotted,
      """MATCH (n:L) WHERE n.prop=42
        |WITH DISTINCT n.prop + 3 AS w
        |MERGE (m:M) ON CREATE SET m.prop=1337 ON MATCH SET m.prop=13337""".stripMargin)

    // Then
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  test("should work in a correlated subquery") {
    val n = createLabeledNode("N")
    val m = createLabeledNode("M")

    val result = executeWith(Configs.InterpretedAndSlotted,
      """MATCH (n:N)
        |CALL {
        |  WITH n
        |  MATCH (m:M)
        |  MERGE (n)-[:REL]->(m)
        |  RETURN m
        |}
        |RETURN n, m""".stripMargin)

    assertStats(result, relationshipsCreated = 1)

    countNodes() shouldBe 2
    countRelationships() shouldBe 1

    result.toList shouldBe List(
      Map("n" -> n, "m" -> m)
    )

    withTx { tx =>
      val endNode = tx.getNodeById(n.getId)
        .getSingleRelationship(RelationshipType.withName("REL"), Direction.OUTGOING)
        .getEndNode
      endNode shouldBe m
    }
  }

  test("should work in a horizon that does not carry over node") {
    val n = createLabeledNode("N")
    val m = createLabeledNode("M")

    val result = executeWith(Configs.InterpretedAndSlotted,
      """MATCH (n:N)
        |WITH 1 AS foo
        |MATCH (m:M)
        |MERGE (n)-[:REL]->(m)
        |RETURN n, m""".stripMargin,
      expectedDifferentResults = Configs.All // Creating the second n, so different results are expected
    )

    assertStats(result, relationshipsCreated = 1, nodesCreated = 1)

    countNodes() shouldBe 3
    countRelationships() shouldBe 1

    result.toList.head("m") should be (m)
    result.toList.head("n") shouldNot be (n)
  }
}
