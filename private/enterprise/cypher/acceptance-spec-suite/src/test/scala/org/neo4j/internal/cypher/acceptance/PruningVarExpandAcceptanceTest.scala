/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class PruningVarExpandAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle query with no predicate") {
    // Given the graph:
    executeSingle("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL]->()-[:REL]->(:Molecule)""".stripMargin)

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
                             """MATCH (:Scaffold)-[:REL*3]->(m:Molecule)
         |RETURN DISTINCT m""".stripMargin)

    // Then
    result.toList should be(empty)
  }

  test("should handle query with predicates") {
    // Given the graph:
    executeSingle("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL {prop:index}]->()-[:REL {prop: index}]->(:Molecule)""".stripMargin)

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
                             """MATCH ()-[:REL*2 {prop:42}]->(m) RETURN DISTINCT m""")

    // Then
    result.toList should have size 1
  }

  test("Pruning var expand should honour the predicate also for the first node") {
    createLabeledNode(Map("bar" -> 2), "Foo")
    val query =
      """
        |MATCH (a:Foo)
        |MATCH path = ( (a)-[:REL*0..2]-(b) )
        |WHERE ALL(n in nodes(path) WHERE n.bar = 1)
        |RETURN DISTINCT b
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList shouldBe empty
  }
}
