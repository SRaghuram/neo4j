/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.morsel.state.{ConcurrentStateFactory, TheExecutionState}
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutablePipeline, WorkerManagement, WorkerResourceProvider}
import org.neo4j.cypher.internal.runtime.{InputDataStream, MemoryTracking, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
  * [[QueryExecutor]] implementation which uses a fixed number (n) of workers to execute
  * query work.
  */
class FixedWorkersQueryExecutor(transactionBinder: TransactionBinder,
                                val workerResourceProvider: WorkerResourceProvider,
                                val workerManager: WorkerManagement)
  extends QueryExecutor {


  // ========== QUERY EXECUTOR ===========

    def assertAllReleased(): Unit = {
      AssertionRunner.runUnderAssertion { () =>
        workerResourceProvider.assertAllReleased()
        workerManager.assertNoWorkerIsActive()
      }
    }

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
                                       memoryTracking: MemoryTracking): ProfiledQuerySubscription = {

    DebugLog.log("FixedWorkersQueryExecutor.execute()")

    val stateFactory = new ConcurrentStateFactory

    val tracer = schedulerTracer.traceQuery()
    val tracker = stateFactory.newTracker(subscriber, queryContext, tracer)

    val queryState = QueryState(params,
                                subscriber,
                                tracker,
                                morselSize,
                                queryIndexes,
                                transactionBinder,
                                workerManager.numberOfWorkers,
                                nExpressionSlots,
                                prePopulateResults,
                                doProfile,
                                inputDataStream)

    val initializationResources = workerResourceProvider.resourcesForWorker(0)
    val executionState = new TheExecutionState(executionGraphDefinition,
                                               executablePipelines,
                                               stateFactory,
                                               workerManager,
                                               queryContext,
                                               queryState,
                                               initializationResources,
                                               tracker)

    val (workersProfiler, queryProfile) =
      if (doProfile) {
        val profiler = new FixedWorkersQueryProfiler(workerManager.numberOfWorkers, executionGraphDefinition.applyRhsPlans)
        (profiler, profiler.Profile)
      } else {
        (WorkersQueryProfiler.NONE, QueryProfile.NONE)
      }

    val executingQuery = new ExecutingQuery(executionState,
                                            queryContext,
                                            queryState,
                                            tracer,
                                            workersProfiler,
                                            workerResourceProvider)

    queryContext.transactionalContext.transaction.freezeLocks()

    executionState.initializeState()
    workerManager.queryManager.addQuery(executingQuery)
    ProfiledQuerySubscription(executingQuery, queryProfile, stateFactory.memoryTracker)
  }
}
