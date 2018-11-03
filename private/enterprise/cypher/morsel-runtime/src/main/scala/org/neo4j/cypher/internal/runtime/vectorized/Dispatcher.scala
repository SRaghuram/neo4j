/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpressionCursors
import org.neo4j.cypher.internal.runtime.parallel.{Scheduler, SchedulerTracer, SingleThreadScheduler}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

class Dispatcher(morselSize: Int, scheduler: Scheduler[ExpressionCursors]) {

  def execute[E <: Exception](operators: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue,
                              schedulerTracer: SchedulerTracer)
                             (visitor: QueryResultVisitor[E]): Unit = {
    val leaf = getLeaf(operators)

    val state = QueryState(params, visitor, morselSize, singeThreaded = scheduler.isInstanceOf[SingleThreadScheduler[_]])

    // instead of creating new cursors here only for initializing the query, we could
    //    a) delegate the task of finding the initial task to the scheduler, and use the schedulers cursors
    //    b) attempt some lazy cursor creation here, because they will usually not be needed
    val initExpressionCursors = new ExpressionCursors(queryContext.transactionalContext.cursors)
    val initialTask =
      try {
        leaf.init(MorselExecutionContext.EMPTY, queryContext, state, initExpressionCursors)
      } finally {
        initExpressionCursors.close()
      }

    val queryExecution = scheduler.execute(initialTask, schedulerTracer)
    val maybeError = queryExecution.await()
    if (maybeError.isDefined)
      throw maybeError.get
  }

  private def getLeaf(pipeline: Pipeline): StreamingPipeline = {
    var leafOp = pipeline
    while (leafOp.upstream.nonEmpty) {
      leafOp = leafOp.upstream.get
    }

    leafOp.asInstanceOf[StreamingPipeline]
  }
}
