/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTestBase

trait ProfilePipelinedMemoryTestBase extends ProfileMemoryTestBase[EnterpriseRuntimeContext] {

  override def assertOnMemory(logicalQuery: LogicalQuery, input: InputValues, numOperators: Int, allocatingOperators: Int*): Unit = {
    val runtimeResult = profile(logicalQuery, runtime, input.stream())
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    for(i <- 0 until numOperators) {
      withClue(s"Memory allocations of plan $i: ") {
        if (allocatingOperators.contains(i)) {
          queryProfile.operatorProfile(i).maxAllocatedMemory() should be > 0L
        }
        // In Pipelined we track memory in more places, which means more operators (that don't really eagerize) will have some memory showing up
        // in the profile. Therefore, we don't assert on these numbers being OperatorProfile.NO_DATA here
      }
    }
    queryProfile.maxAllocatedMemory() should be > 0L
  }

  test("should profile memory of cartesian product query") {
    given {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 4, 1)
  }

  test("should profile memory in apply buffer") {
    given {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 4, 2)
  }

  test("should profile memory in optional buffer") {
    given {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optional()
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of triadic selection") {
    // given
    given { chainGraphs(3, "A", "B") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.expandAll("(y)-->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 8, 6, 8) // operator ids are shifted because of rewriting
  }
}

trait ProfilePipelinedNoFusingMemoryTestBase extends ProfilePipelinedMemoryTestBase {

  test("should profile memory in morsel buffer") {
    given {
      circleGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }
}
