/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.{Scheduler, SchedulerTracer, SingleThreadScheduler}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

class Dispatcher(morselSize: Int, scheduler: Scheduler) {

  def execute[E <: Exception](operators: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue,
                              schedulerTracer: SchedulerTracer)
                             (visitor: QueryResultVisitor[E]): Unit = {
    val leaf = getLeaf(operators)

    val state = QueryState(params, visitor, morselSize, singeThreaded = scheduler.isInstanceOf[SingleThreadScheduler])
    val initialTask = leaf.init(MorselExecutionContext.EMPTY, queryContext, state)
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
