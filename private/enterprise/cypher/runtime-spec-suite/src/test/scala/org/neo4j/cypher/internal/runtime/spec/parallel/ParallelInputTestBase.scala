/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.InputTestBase

abstract class ParallelInputTestBase(edition: Edition[EnterpriseRuntimeContext],
                                     runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends InputTestBase(edition, runtime, SIZE_HINT) {

  test("should process input batches in parallel") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unwind("[1] AS whoCares")
      .nonFuseable()
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(nBatches = SIZE_HINT, batchSize = 2, rowNumber => rowNumber)

    // then
    val result = execute(logicalQuery, runtime, input)
    result should beColumns("x").withRows(input.flatten)

    // and
    def try100times(): Unit = {
      val condition = ENTERPRISE.HAS_EVIDENCE_OF_PARALLELISM
      val nAttempts = 100
      for (_ <- 0 until nAttempts) {
        val (result, context) = executeAndContext(logicalQuery, runtime, input)
        //TODO here we should not materialize the result
        result.awaitAll()
        if (condition.test(context))
          return
      }
      fail(s"${condition.errorMsg} in $nAttempts attempts!")
    }
    try100times()
  }
}
