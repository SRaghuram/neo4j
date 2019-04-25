/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.InputTestBase
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, LogicalQueryBuilder}
import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

abstract class ParallelInputTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends InputTestBase(ENTERPRISE.PARALLEL, runtime, SIZE_HINT) {

  test("should process input batches in parallel") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(nBatches = SIZE_HINT, batchSize = 2, rowNumber => rowNumber)

    // then
    val result = execute(logicalQuery, runtime, input)
    result should beColumns("x").withRows(input.flatten)

    // and
    executeAndAssertCondition(logicalQuery, input, ENTERPRISE.HAS_EVIDENCE_OF_PARALLELISM)
  }
}
