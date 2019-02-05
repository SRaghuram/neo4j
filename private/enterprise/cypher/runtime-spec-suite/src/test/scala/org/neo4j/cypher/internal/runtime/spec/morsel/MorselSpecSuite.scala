/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE_EDITION.HasEvidenceOfParallelism
import org.neo4j.cypher.internal.{LogicalQuery, MorselRuntime}
import org.neo4j.cypher.internal.runtime.spec.tests.{AggregationTestBase, AllNodeScanTestBase, InputTestBase}
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_EDITION, LogicalQueryBuilder}
import org.neo4j.cypher.result.RuntimeResult

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

class MorselInputTest extends InputTestBase(ENTERPRISE_EDITION, MorselRuntime) {

  test("should process input batches in parallel") {
    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("x")
      .input("x")
      .build()

    val input = inputSingleColumn(nBatches = 10000, batchSize = 2, rowNumber => rowNumber)

    val result = executeUntil(logicalQuery, input, HasEvidenceOfParallelism)

    // then
    result should beColumns("x").withRows(input.flatten)
  }
}

class MorselAggregationTest extends AggregationTestBase(ENTERPRISE_EDITION, MorselRuntime)
