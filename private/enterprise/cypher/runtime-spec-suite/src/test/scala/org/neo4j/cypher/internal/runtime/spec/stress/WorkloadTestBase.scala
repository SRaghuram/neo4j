/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.AnyValue

abstract class WorkloadTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                           runtime: CypherRuntime[CONTEXT],
                                                           sizeHint: Int
                                                          ) extends RuntimeTestSuite[CONTEXT](edition, runtime, true) {

  test("should deal with concurrent queries") {
    // given
    val nodes = nodeGraph(10)
    restartTx()
    val executor = Executors.newFixedThreadPool(8)
    val QUERIES_PER_THREAD = 50

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val futureResultSets =
      (0 until 8).map(_ =>
        executor.submit(new Callable[Seq[IndexedSeq[Array[AnyValue]]]] {
          override def call(): Seq[IndexedSeq[Array[AnyValue]]] = {
            for (_ <- 0 until QUERIES_PER_THREAD)
              yield executeAndConsumeTransactionally(logicalQuery, runtime)
          }
        })
      )

    // then
    val expected = singleColumn(nodes)

    for (futureResultSet <- futureResultSets) {
      val resultSet = futureResultSet.get(1, TimeUnit.MINUTES)
      resultSet.size should be(QUERIES_PER_THREAD)
      for (result <- resultSet) {
        expected.matchesRaw(Array("x"), result) shouldBe true
      }
    }

    executor.shutdownNow()
  }

}
