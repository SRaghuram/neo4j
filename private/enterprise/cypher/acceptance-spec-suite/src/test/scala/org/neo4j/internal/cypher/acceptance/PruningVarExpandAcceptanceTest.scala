/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class PruningVarExpandAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle query with no predicate") {
    // Given the graph:
    graph.execute("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL]->()-[:REL]->(:Molecule)""".stripMargin)

    // When
    val result = executeWith( Configs.InterpretedAndSlotted,
       """MATCH (:Scaffold)-[:REL*3]->(m:Molecule)
         |RETURN DISTINCT m""".stripMargin)

    // Then
    result.toList should be(empty)
  }

  test("should handle query with predicates") {
    // Given the graph:
    graph.execute("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL {prop:index}]->()-[:REL {prop: index}]->(:Molecule)""".stripMargin)

    // When
    val result = executeWith( Configs.InterpretedAndSlotted,
                              """MATCH ()-[:REL*2 {prop:42}]->(m) RETURN DISTINCT m""")

    // Then
    result.toList should have size 1
  }
}
