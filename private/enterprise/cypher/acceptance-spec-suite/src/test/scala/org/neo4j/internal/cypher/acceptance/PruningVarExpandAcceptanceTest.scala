/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class PruningVarExpandAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle all predicate in optional match") {
    // Given the graph:
    graph.execute("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL]->()-[:REL]->(:Molecule)""".stripMargin)

    // When
    val result = executeSingle( // using innerExecute because 3.2 gives stack overflow
      s"""CYPHER runtime=interpreted
         |MATCH (:Scaffold)-[:REL*3]->(m:Molecule)
         |RETURN DISTINCT m""".stripMargin, Map.empty)

    // Then
    result.toList should be(empty)
  }
}
