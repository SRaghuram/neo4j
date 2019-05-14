/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PipelineBuilderTest extends CypherFunSuite {

  test("should") {
    val break = BREAK_FOR_LEAFS
    val physicalPlan = new PhysicalPlanBuilder(break)
      .produceResults("n")
      .allNodeScan("n")
      .build()

    val executionStateDefinition = new PipelineBuilder(break, physicalPlan).build()
    // TODO add assertion on executionStateDefinition
  }
}
