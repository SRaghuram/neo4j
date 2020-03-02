/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.Worker
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.kernel.impl.query.QuerySubscription

class CallingThreadExecutingQuery(executionState: ExecutionState,
                                  queryState: PipelinedQueryState,
                                  queryExecutionTracer: QueryExecutionTracer,
                                  workersQueryProfiler: WorkersQueryProfiler,
                                  worker: Worker,
                                  workerResourceProvider: WorkerResourceProvider,
                                  executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy)
  extends ExecutingQuery(executionState, queryState, queryExecutionTracer, workersQueryProfiler, workerResourceProvider, executionGraphSchedulingPolicy)
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
      checkOnlyWhenAssertionsAreEnabled(worker.assertIsNotActive() &&
        workerResourceProvider.assertAllReleased())
    } finally {
      workerResourceProvider.shutdown()
    }
  }

  override def await(): Boolean = {
    if (executionState.hasEnded) {
      try {
        checkOnlyWhenAssertionsAreEnabled(worker.assertIsNotActive() && workerResourceProvider.assertAllReleased())
      } finally {
        workerResourceProvider.shutdown()
      }
    }

    super.await()
  }
}
