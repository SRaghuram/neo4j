/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutionState, Worker}
import org.neo4j.kernel.impl.query.QuerySubscription

class CallingThreadExecutingQuery(executionState: ExecutionState,
                                  queryContext: QueryContext,
                                  queryState: QueryState,
                                  queryExecutionTracer: QueryExecutionTracer,
                                  workersQueryProfiler: WorkersQueryProfiler,
                                  worker: Worker)
  extends ExecutingQuery(executionState, queryContext, queryState, queryExecutionTracer, workersQueryProfiler)
  with QuerySubscription {
  private val flowControl = queryState.flowControl

  override def request(numberOfRecords: Long): Unit = {
    flowControl.request(numberOfRecords)
    while (!executionState.isCompleted && flowControl.hasDemand) {
      worker.workOnQuery(this)
    }
  }

  override def cancel(): Unit = {
    // We have to check this before we call cancel on the floe control
    val isCompleted = executionState.isCompleted
    flowControl.cancel()
    if (!isCompleted) {
      executionState.cancelQuery(worker.resources)
    }
    try {
      worker.assertAllReleased()
    } finally {
      worker.close()
    }
  }

  override def await(): Boolean = {
    if (executionState.isCompleted) {
      try {
        worker.assertAllReleased()
      } finally {
        worker.close()
      }
    }

    flowControl.await()
  }
}
