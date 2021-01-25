/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.stress.ParallelStressSuite.MORSEL_SIZE
import org.neo4j.cypher.internal.runtime.spec.stress.ParallelStressSuite.WORKERS
import org.neo4j.exceptions.ArithmeticException

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object ParallelErrorHandlingTestBase {
  val MORSEL_SIZE = 1000
  val WORKERS = 10
}

abstract class ParallelErrorHandlingTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends RuntimeTestSuite(ENTERPRISE.WITH_NO_FUSING(ENTERPRISE.DEFAULT).copyWith(
    GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(MORSEL_SIZE),
    GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(MORSEL_SIZE),
    GraphDatabaseInternalSettings.cypher_worker_count -> Integer.valueOf(WORKERS)), runtime) {

  test("should complete query with concurrent errors and close cursors") {
    given {
      nodePropertyGraph(MORSEL_SIZE * WORKERS, {
        case i => Map("prop" -> (i + 1) % MORSEL_SIZE)
      })
    }

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .expand("(n)--(n)") // Force a new pipeline so the AllNodeScan can be parallel
      .filter("100/n.prop = 1") // will explode!
      .allNodeScan("n")
      .build()

    // when
    val futureResult = Future(consume(execute(logicalQuery, runtime)))(global)

    // then
    intercept[ArithmeticException] {
      Await.result(futureResult, 30.seconds)
    }
  }
}
