/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class DebugAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport{

  test("CYPHER DEBUG=dumpcosts should dump costs") {
    graph.createIndex("Person", "number")
    val x = createLabeledNode(Map("name" -> "x", "number" -> 0), "Person")
    val y = createLabeledNode(Map("name" -> "y", "number" -> 1), "Person")
    val z = createLabeledNode(Map("name" -> "z", "number" -> 2), "Person")
    relate(x, y)
    relate(y, z)

    val query =
      """CYPHER DEBUG=dumpcosts MATCH(n)-[]-(p) WHERE n.number = 2 RETURN n""".stripMargin

      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
      result.columns should contain theSameElementsInOrderAs List("#", "planId", "planText", "planCost", "cost", "est cardinality", "winner")
      result.toSet should have size(19)
      result.head("winner") shouldBe "WON"
    }

}
