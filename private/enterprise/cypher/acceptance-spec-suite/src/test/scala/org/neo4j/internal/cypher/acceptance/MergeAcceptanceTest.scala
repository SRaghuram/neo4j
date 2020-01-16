/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.Relationship
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

  test("should give sensible error message on add relationship to null node") {
    val query =
      """OPTIONAL MATCH (a)
        |MERGE (a)-[r:X]->()
      """.stripMargin

    failWithError(Configs.InterpretedAndSlotted, query, Seq(
      "Expected to find a node, but found instead: null",
      "Expected to find a node at a but found nothing Some(null)",
      "Failed to create relationship `r`, node `a` is missing. " +
        "If you prefer to simply ignore rows where a relationship node is missing, " +
        "set 'cypher.lenient_create_relationship = true' in neo4j.conf"))
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
}
