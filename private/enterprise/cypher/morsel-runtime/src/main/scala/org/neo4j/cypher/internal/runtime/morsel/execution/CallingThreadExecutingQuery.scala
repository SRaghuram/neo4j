/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutionState, Worker, WorkerResourceProvider}
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.kernel.impl.query.QuerySubscription

class CallingThreadExecutingQuery(executionState: ExecutionState,
                                  queryContext: QueryContext,
                                  queryState: QueryState,
                                  queryExecutionTracer: QueryExecutionTracer,
                                  workersQueryProfiler: WorkersQueryProfiler,
                                  worker: Worker,
                                  workerResourceProvider: WorkerResourceProvider,
                                  schedulingPolicy: SchedulingPolicy)
  extends ExecutingQuery(executionState, queryContext, queryState, queryExecutionTracer, workersQueryProfiler, workerResourceProvider, schedulingPolicy)
  with QuerySubscription {

  private val workerResources = workerResourceProvider.resourcesForWorker(worker.workerId)

  override def request(numberOfRecords: Long): Unit = {
    super.request(numberOfRecords)
    while (!executionState.hasEnded && flowControl.hasDemand) {
      val worked = worker.workOnQuery(this, workerResources)
      if (!worked && !executionState.hasEnded && flowControl.hasDemand) {
        return
      }
    }
  }

  override def cancel(): Unit = {
    // We have to check this before we call cancel on the flow control
    val hasEnded = executionState.hasEnded
    flowControl.cancel()
    if (!hasEnded) {
      executionState.cancelQuery(workerResources)
    }
    try {
      if (AssertionRunner.isAssertionsEnabled) {
        worker.assertIsNotActive()
        workerResourceProvider.assertAllReleased()
      }
    } finally {
      workerResourceProvider.shutdown()
    }
  }

  override def await(): Boolean = {
    if (executionState.hasEnded) {
      try {
        if (AssertionRunner.isAssertionsEnabled) {
          worker.assertIsNotActive()
          workerResourceProvider.assertAllReleased()
        }
      } finally {
        workerResourceProvider.shutdown()
      }
    }

    super.await()
  }
}
