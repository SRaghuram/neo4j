/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutionState, WorkerResourceProvider}
import org.neo4j.kernel.impl.query.QuerySubscription

class ExecutingQuery(val executionState: ExecutionState,
                     val queryContext: QueryContext,
                     val queryState: QueryState,
                     val queryExecutionTracer: QueryExecutionTracer,
                     val workersQueryProfiler: WorkersQueryProfiler,
                     val workerResourceProvider: WorkerResourceProvider) extends QuerySubscription {
  protected val flowControl: FlowControl = queryState.flowControl

  def bindTransactionToThread(): Unit =
    queryState.transactionBinder.bindToThread(queryContext.transactionalContext.transaction)

  def unbindTransaction(): Unit =
    queryState.transactionBinder.unbindFromThread()

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

  override def toString: String = s"ExecutingQuery ${System.identityHashCode(this)}"
}
