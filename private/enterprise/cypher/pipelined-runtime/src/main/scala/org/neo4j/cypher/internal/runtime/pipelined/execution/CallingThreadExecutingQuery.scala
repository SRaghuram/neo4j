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

  sealed trait STATE
  case object Idle extends STATE
  case object Working extends STATE
  case class CancelledWhileWorking(hasEnded: Boolean) extends STATE

  private var state: STATE = Idle

  override def request(numberOfRecords: Long): Unit = {
    super.request(numberOfRecords)
    while (!executionState.hasEnded && flowControl.hasDemand) {
      try {
        state = Working
        val worked = worker.workOnQuery(this, workerResources)
        if (!worked && !executionState.hasEnded && flowControl.hasDemand) {
          return
        }
      } finally {
        state match {
          case CancelledWhileWorking(hasEnded) => cleanUpOnCancel(hasEnded)
          case _ =>
        }
        state = Idle
      }
    }
  }

  override def cancel(): Unit = {
    // We have to check this before we call cancel on the flow control
    val hasEnded = executionState.hasEnded
    flowControl.cancel()
    if (state == Idle) {
      cleanUpOnCancel(hasEnded)
    } else {
      state = CancelledWhileWorking(hasEnded)
    }
  }

  private def cleanUpOnCancel(hasEnded: Boolean): Unit = {
    if (!hasEnded) {
      executionState.cancelQuery(workerResources)
    }
    assertClosedAndShutdown()
  }

  override def await(): Boolean = {
    if (executionState.hasEnded) {
      assertClosedAndShutdown()
    }
    super.await()
  }

  private def assertClosedAndShutdown(): Unit = {
    try {
      checkOnlyWhenAssertionsAreEnabled(worker.assertIsNotActive() &&
        workerResourceProvider.assertAllReleased())
    } finally {
      workerResourceProvider.shutdown()
    }
  }
}
