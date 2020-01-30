/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.compiled

import org.neo4j.cypher.internal.CompiledRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.compiled.CompiledSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeLockingUniqueIndexSeekTestBase

object CompiledSpecSuite {
  val SIZE_HINT = 200
}

class CompiledAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.DEFAULT, CompiledRuntime, SIZE_HINT)
class CompiledAggregationTest extends RuntimeTestSuite(ENTERPRISE.DEFAULT, CompiledRuntime) {
  // Compiled only supports count, thus not extending AggregationTestBase
  test("should count(n.prop)") {
    given {
      nodePropertyGraph(10000, {
        case i: Int if i % 2 == 0 => Map("num" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(5000)
  }
}
class CompiledExpandAllTest extends ExpandAllTestBase(ENTERPRISE.DEFAULT, CompiledRuntime, SIZE_HINT)
class CompiledExpandIntoTest extends ExpandIntoTestBase(ENTERPRISE.DEFAULT, CompiledRuntime, SIZE_HINT)
class CompiledLabelScanTest extends LabelScanTestBase(ENTERPRISE.DEFAULT, CompiledRuntime, SIZE_HINT)
class CompiledNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.DEFAULT, CompiledRuntime, SIZE_HINT)
                                with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
