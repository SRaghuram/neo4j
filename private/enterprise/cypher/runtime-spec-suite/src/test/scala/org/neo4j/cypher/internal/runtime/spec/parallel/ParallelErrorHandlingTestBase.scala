/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.runtime.spec.stress.ParallelStressSuite.{MORSEL_SIZE, WORKERS}
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}
import org.neo4j.exceptions.ArithmeticException

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

object ParallelErrorHandlingTestBase {
  val MORSEL_SIZE = 1000
  val WORKERS = 10
}

abstract class ParallelErrorHandlingTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends RuntimeTestSuite(ENTERPRISE.NO_FUSING.copyWith(
    GraphDatabaseSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(MORSEL_SIZE),
    GraphDatabaseSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(MORSEL_SIZE),
    GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(WORKERS)), runtime) {

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
    import scala.concurrent.ExecutionContext.global
    val futureResult = Future(consume(execute(logicalQuery, runtime)))(global)

    // then
    intercept[ArithmeticException] {
      Await.result(futureResult, 30.seconds)
    }
  }
}
