/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.kernel.impl.query.QuerySubscription

class ExecutingQuery(val executionState: ExecutionState,
                     val queryState: PipelinedQueryState,
                     val queryExecutionTracer: QueryExecutionTracer,
                     val workersQueryProfiler: WorkersQueryProfiler,
                     val workerResourceProvider: WorkerResourceProvider,
                     executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy,
                     stateFactory: StateFactory) extends QuerySubscription {

  val querySchedulingPolicy: QuerySchedulingPolicy = executionGraphSchedulingPolicy.querySchedulingPolicy(this, stateFactory)

  protected val flowControl: FlowControl = queryState.flowControl

  override def request(numberOfRecords: Long): Unit = {
    flowControl.request(numberOfRecords)
  }

  override def cancel(): Unit = {
    flowControl.cancel()
    executionState.scheduleCancelQuery()
  }

  override def await(): Boolean = {
    flowControl.await()
  }

  //noinspection AccessorLikeMethodIsEmptyParen
  def hasSucceeded(): Boolean = {
    executionState.hasSucceeded
  }

  override def toString: String = s"ExecutingQuery ${System.identityHashCode(this)}"
}
