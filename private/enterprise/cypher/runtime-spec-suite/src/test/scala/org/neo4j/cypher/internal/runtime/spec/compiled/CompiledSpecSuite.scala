/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.compiled

import org.neo4j.cypher.internal.runtime.spec.compiled.CompiledSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.{AllNodeScanTestBase, ExpandAllTestBase, LabelScanTestBase, NodeIndexSeekTestBase, NodeLockingUniqueIndexSeekTestBase}
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_SINGLE_THREAD, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.{CompiledRuntime, EnterpriseRuntimeContext}

object CompiledSpecSuite {
  val SIZE_HINT = 200
}

class CompiledAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_SINGLE_THREAD, CompiledRuntime, SIZE_HINT)
class CompiledAggregationTest extends RuntimeTestSuite(ENTERPRISE_SINGLE_THREAD, CompiledRuntime) {
  // Compiled only supports count, thus not extending AggregationTestBase
  test("should count(n.prop)") {
    // given
    nodePropertyGraph(10000, {
      case i: Int if i % 2 == 0 => Map("num" -> i)
    }, "Honey")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Map.empty, Map("c" -> count(prop("x", "num"))))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRow(5000)
  }
}
class CompiledExpandAllTest extends ExpandAllTestBase(ENTERPRISE_SINGLE_THREAD, CompiledRuntime, SIZE_HINT)
class CompiledLabelScanTest extends LabelScanTestBase(ENTERPRISE_SINGLE_THREAD, CompiledRuntime, SIZE_HINT)
class CompiledNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE_SINGLE_THREAD, CompiledRuntime, SIZE_HINT)
                                with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
