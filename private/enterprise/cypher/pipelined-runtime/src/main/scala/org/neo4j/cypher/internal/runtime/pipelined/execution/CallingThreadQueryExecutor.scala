/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MEMORY_BOUND
import org.neo4j.cypher.internal.runtime.MEMORY_TRACKING
import org.neo4j.cypher.internal.runtime.MemoryTracking
import org.neo4j.cypher.internal.runtime.NO_TRACKING
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.pipelined.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.pipelined.Sleeper
import org.neo4j.cypher.internal.runtime.pipelined.Worker
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.state.MemoryTrackingStandardStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.TheExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
 * Single threaded implementation of [[QueryExecutor]]. Executes the query on
 * the thread which calls execute, without any synchronization with other queries
 * or any parallel execution.
 */
class CallingThreadQueryExecutor(cursors: CursorFactory) extends QueryExecutor with WorkerWaker {

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
                                       doProfile: Boolean,
                                       morselSize: Int,
                                       memoryTracking: MemoryTracking,
                                       executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy): ProfiledQuerySubscription = {

    DebugLog.log("CallingThreadQueryExecutor.execute()")

    val stateFactory =
      memoryTracking match {
        case NO_TRACKING => new StandardStateFactory
        case MEMORY_TRACKING => new MemoryTrackingStandardStateFactory(Long.MaxValue)
        case MEMORY_BOUND(maxAllocatedBytes) => new MemoryTrackingStandardStateFactory(maxAllocatedBytes)
      }

    val resources = new QueryResources(cursors: CursorFactory, queryContext.transactionalContext.transaction.pageCursorTracer())
    val tracer = schedulerTracer.traceQuery()
    val tracker = stateFactory.newTracker(subscriber, queryContext, tracer)
    val queryState = QueryState(queryContext,
                                params,
                                subscriber,
                                tracker,
                                morselSize,
                                queryIndexes,
                                numberOfWorkers = 1,
                                nExpressionSlots,
                                prePopulateResults,
                                doProfile,
                                inputDataStream)

    val executionState = new TheExecutionState(executionGraphDefinition,
      executablePipelines,
      stateFactory,
      this,
      queryState,
      resources,
      tracker)

    executionState.initializeState()

    val (workersProfiler, queryProfile) =
      if (doProfile) {
        val profiler = new FixedWorkersQueryProfiler(1, executionGraphDefinition.applyRhsPlans, stateFactory.memoryTracker)
        (profiler, profiler.Profile)
      } else {
        (WorkersQueryProfiler.NONE, QueryProfile.NONE)
      }

    val worker = new Worker(0, null, Sleeper.noSleep)
    val workerResourceProvider = new WorkerResourceProvider(1, () => resources)
    val executingQuery = new CallingThreadExecutingQuery(executionState,
      queryState,
      tracer,
      workersProfiler,
      worker,
      workerResourceProvider,
      executionGraphSchedulingPolicy)
    ProfiledQuerySubscription(executingQuery, queryProfile, stateFactory.memoryTracker)
  }
}
