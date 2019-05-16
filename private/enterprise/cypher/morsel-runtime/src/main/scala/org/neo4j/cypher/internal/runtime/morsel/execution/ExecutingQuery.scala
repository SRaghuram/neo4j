/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.ExecutionState
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer

class ExecutingQuery(val executionState: ExecutionState,
                     val queryContext: QueryContext,
                     val queryState: QueryState,
                     val queryExecutionTracer: QueryExecutionTracer) {

  def bindTransactionToThread(): Unit =
    queryState.transactionBinder.bindToThread(queryContext.transactionalContext.transaction)

  def unbindTransaction(): Unit =
    queryState.transactionBinder.unbindFromThread()
}
