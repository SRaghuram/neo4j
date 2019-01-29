/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.compiled

import org.neo4j.cypher.internal.CompiledRuntime
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_EDITION, LogicalQueryBuilder, RuntimeTestSuite}

class CompiledAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_EDITION, CompiledRuntime)
class CompiledAggregationTest extends RuntimeTestSuite(ENTERPRISE_EDITION, CompiledRuntime) {
  // Compiled only supports count, thus not extending AggregationTestBase
  test("should count(n.prop)") {
    // given
    nodePropertyGraph(10000, {
      case i: Int if i % 2 == 0 => Map("num" -> i)
    }, "Honey")

    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("c")
      .aggregation(Map.empty, Map("c" -> count(prop("x", "num"))))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRow(5000)
  }
}
