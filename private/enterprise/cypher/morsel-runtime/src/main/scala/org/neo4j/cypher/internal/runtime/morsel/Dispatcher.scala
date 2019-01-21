/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.scheduling.{Scheduler, SchedulerTracer}
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.MapValue

class Dispatcher(morselSize: Int, scheduler: Scheduler[QueryResources], transactionBinder: TransactionBinder) {

  def execute[E <: Exception](pipeline: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue,
                              schedulerTracer: SchedulerTracer,
                              queryIndexes: Array[IndexReadSession])
                             (visitor: QueryResultVisitor[E]): Unit = {
    val leafPipeline = pipeline.getUpstreamLeafPipeline

    val state = QueryState(params,
                           visitor,
                           morselSize,
                           queryIndexes,
                           transactionBinder,
                           scheduler.numberOfWorkers)

    // instead of creating new cursors here only for initializing the query, we could
    //    a) delegate the task of finding the initial task to the scheduler, and use the schedulers cursors
    //    b) attempt some lazy cursor creation here, because they will usually not be needed
    val initResources = new QueryResources(queryContext.transactionalContext.cursors)

    // NOTE: We will iterate over this initial input MorselExecutionContext, so we must clone MorselExecutionContext.EMPTY
    val initialTasks =
      try {
        leafPipeline.init(MorselExecutionContext.createSingleRow(), queryContext, state, initResources)
      } finally {
        initResources.close()
      }

    val queryExecution = scheduler.execute(schedulerTracer, initialTasks)
    val maybeError = queryExecution.await()
    if (maybeError.isDefined)
      throw maybeError.get
  }
}
