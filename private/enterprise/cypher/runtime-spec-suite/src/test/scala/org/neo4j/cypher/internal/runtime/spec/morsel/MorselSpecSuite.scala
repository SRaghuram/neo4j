/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE_PARALLEL.HasEvidenceOfParallelism
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.{AggregationTestBase, AllNodeScanTestBase, ExpandAllTestBase, InputTestBase, LabelScanTestBase, NodeIndexScanTestBase, NodeIndexSeekRangeAndCompositeTestBase, NodeIndexSeekTestBase}
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_PARALLEL, LogicalQueryBuilder}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, MorselRuntime}

object MorselSpecSuite {
  val SIZE_HINT = 10000
}

class MorselAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)
class MorselAggregationTest extends AggregationTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)
class MorselExpandAllTest extends ExpandAllTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)
class MorselLabelScanTest extends LabelScanTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)
class MorselNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)
class MorselNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)
                              with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]

class MorselInputTest extends InputTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT) {

  test("should process input batches in parallel") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .input("x")
      .build()

    val input = inputSingleColumn(nBatches = SIZE_HINT, batchSize = 2, rowNumber => rowNumber)

    val result = executeUntil(logicalQuery, input, HasEvidenceOfParallelism)

    // then
    result should beColumns("x").withRows(input.flatten)
  }
}
