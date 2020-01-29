/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTestBase
import org.neo4j.cypher.result.OperatorProfile

trait ProfileSlottedMemoryTestBase {
  self: ProfileMemoryTestBase[EnterpriseRuntimeContext] =>

  test("should profile memory of primitive grouping aggregation") {
    given {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, 3, 1)
  }

  test("should profile memory of primitive distinct") {
    given {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x", "x AS y")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, 3, 1)
  }

  test("should profile memory of single primitive distinct") {
    given {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, 3, 1)
  }
}
