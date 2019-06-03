/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.morsel.state.{StandardStateFactory, TheExecutionState}
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutablePipeline, Worker}
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue

/**
  * Single threaded implementation of [[QueryExecutor]]. Executes the query on
  * the thread which calls execute, without any synchronization with other queries
  * or any parallel execution.
  */
class CallingThreadQueryExecutor(morselSize: Int, transactionBinder: TransactionBinder) extends QueryExecutor with WorkerWaker {

  override def wakeOne(): Unit = ()

  // The calling thread handles it's own resources, so there is
  // no way to assert centrally.
  override def assertAllReleased(): Unit = ()

  override def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                                       executionGraphDefinition: ExecutionGraphDefinition,
                                       inputDataStream: InputDataStream,
                                       queryContext: QueryContext,
                                       params: Array[AnyValue],
                                       schedulerTracer: SchedulerTracer,
                                       queryIndexes: Array[IndexReadSession],
                                       nExpressionSlots: Int,
                                       prePopulateResults: Boolean,
                                       subscriber: QuerySubscriber,
                                       doProfile: Boolean): (QuerySubscription, QueryProfile) = {

    DebugLog.log("CallingThreadQueryExecutor.execute()")

    val resources = new QueryResources(queryContext.transactionalContext.cursors)
    val tracer = schedulerTracer.traceQuery()
    val tracker = StandardStateFactory.newTracker(subscriber, queryContext, tracer)
    val queryState = QueryState(params,
                                subscriber,
                                tracker,
                                morselSize,
                                queryIndexes,
                                transactionBinder,
                                numberOfWorkers = 1,
                                nExpressionSlots,
                                prePopulateResults,
                                inputDataStream)

    val executionState = new TheExecutionState(executionGraphDefinition,
                                               executablePipelines,
                                               StandardStateFactory,
                                               this,
                                               queryContext,
                                               queryState,
                                               resources,
                                               tracker)

    executionState.initializeState()

    val profiler =
      if (doProfile)
        new FixedWorkersQueryProfiler(1)
      else
        WorkersQueryProfiler.NONE

    val worker = new Worker(0, null, LazyScheduling, resources)
    val executingQuery = new CallingThreadExecutingQuery(executionState,
                                                         queryContext,
                                                         queryState,
                                                         tracer,
                                                         profiler,
                                                         worker)
    (executingQuery, profiler)
  }
}
