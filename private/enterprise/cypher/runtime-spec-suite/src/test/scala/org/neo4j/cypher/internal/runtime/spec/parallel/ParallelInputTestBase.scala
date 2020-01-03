/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.InputTestBase
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, Edition, LogicalQueryBuilder}
import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

abstract class ParallelInputTestBase(edition: Edition[EnterpriseRuntimeContext],
                                     runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends InputTestBase(edition, runtime, SIZE_HINT) {

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
