/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.morsel.QueryState
import org.neo4j.cypher.internal.runtime.scheduling.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.zombie.{ExecutionState, Worker}
import org.neo4j.kernel.impl.query.QuerySubscription

class CallingThreadExecutingQuery(executionState: ExecutionState,
                                  queryContext: QueryContext,
                                  queryState: QueryState,
                                  queryExecutionTracer: QueryExecutionTracer,
                                  worker: Worker)
  extends ExecutingQuery(executionState, queryContext, queryState, queryExecutionTracer)
  with QuerySubscription {
  private val flowControl = queryState.flowControl

  override def request(numberOfRecords: Long): Unit = {
    flowControl.request(numberOfRecords)
  }

  override def cancel(): Unit = {
    flowControl.cancel()
  }

  override def await(): Boolean = {
    //TODO: move this to request? (this is consistent to how it is done in ReactiveIterator)
    while (!executionState.isCompleted && flowControl.hasDemand) {
      worker.workOnQuery(this)
    }

    flowControl.await()
  }
}
