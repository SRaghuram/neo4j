/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.kernel.impl.query.QuerySubscription

class ExecutingQuery(val executionState: ExecutionState,
                     val queryState: PipelinedQueryState,
                     val queryExecutionTracer: QueryExecutionTracer,
                     val workersQueryProfiler: WorkersQueryProfiler,
                     val workerResourceProvider: WorkerResourceProvider,
                     executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy) extends QuerySubscription {

  val querySchedulingPolicy: QuerySchedulingPolicy = executionGraphSchedulingPolicy.querySchedulingPolicy(this)

  protected val flowControl: FlowControl = queryState.flowControl

  override def request(numberOfRecords: Long): Unit = {
    flowControl.request(numberOfRecords)
  }

  override def cancel(): Unit = {
    flowControl.cancel()
    executionState.scheduleCancelQuery()
  }

  override def await(): Boolean = {
    try {
      flowControl.await()
    } catch {
      case e: InterruptedException =>
        val e1 = new InterruptedException(toString)
        e1.addSuppressed(e)
        throw e1
    }
  }

  def hasSucceeded: Boolean = {
    executionState.hasSucceeded
  }

  override def toString: String = s"${getClass.getSimpleName}(${System.identityHashCode(this)}, $executionState)"
}
