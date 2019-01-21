/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.MorselRuntime
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_EDITION, LogicalQueryBuilder}
import org.neo4j.cypher.internal.runtime.spec.interpreted.AllNodeScanTestBase

class MorselAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_EDITION, MorselRuntime) {

  test("should handle large scan") {
    // given
    val nodes = nodeGraph(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleValueRows(nodes)
  }
}
