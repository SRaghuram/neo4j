/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTestBase

trait ProfilePipelinedMemoryTestBase extends ProfileMemoryTestBase[EnterpriseRuntimeContext] {

  override def assertOnMemory(logicalQuery: LogicalQuery, numOperators: Int, allocatingOperators: Int*): Unit = {
    val runtimeResult = profile(logicalQuery, runtime)
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
    assertOnMemory(logicalQuery, 4, 1)
  }

  test("should profile memory in morsel buffer") {
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
    assertOnMemory(logicalQuery, 4, 2)
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
    assertOnMemory(logicalQuery, 3, 1)
  }
}
