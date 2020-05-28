/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MemoryTracking
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.pipelined.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.TheExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
  * [[QueryExecutor]] implementation which uses a fixed number (n) of workers to execute
  * query work.
  */
class FixedWorkersQueryExecutor(val workerResourceProvider: WorkerResourceProvider,
                                val workerManager: WorkerManagement)
  extends QueryExecutor {


  // ========== QUERY EXECUTOR ===========

    def assertAllReleased(): Unit = {
      checkOnlyWhenAssertionsAreEnabled(workerResourceProvider.assertAllReleased() &&
                        workerManager.assertNoWorkerIsActive())

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
                                       memoryTracking: MemoryTracking,
                                       executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy): ProfiledQuerySubscription = {

    if (queryContext.transactionalContext.dataRead.transactionStateHasChanges) {
      throw new RuntimeUnsupportedException("The parallel runtime is not supported if there are changes in the transaction state. Use another runtime.")
    }

    if (!workerManager.hasWorkers) {
      throw new RuntimeUnsupportedException("There are no workers configured for the parallel runtime. " +
        s"Change '${GraphDatabaseInternalSettings.cypher_worker_count.name()}' to something other than -1 to use the parallel runtime.")
    }

    DebugLog.log("FixedWorkersQueryExecutor.execute()")

    val stateFactory = new ConcurrentStateFactory

    val tracer = schedulerTracer.traceQuery()
    val tracker = stateFactory.newTracker(subscriber, queryContext, tracer)

    val queryState = PipelinedQueryState(queryContext,
                                params,
                                subscriber,
                                tracker,
                                morselSize,
                                queryIndexes,
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
                                               queryState,
                                               initializationResources,
                                               tracker)

    val (workersProfiler, queryProfile) =
      if (doProfile) {
        val profiler = new FixedWorkersQueryProfiler(workerManager.numberOfWorkers, executionGraphDefinition.applyRhsPlans, stateFactory.memoryTracker)
        (profiler, profiler.Profile)
      } else {
        (WorkersQueryProfiler.NONE, QueryProfile.NONE)
      }

    val executingQuery = new ExecutingQuery(executionState,
                                            queryState,
                                            tracer,
                                            workersProfiler,
                                            workerResourceProvider,
                                            executionGraphSchedulingPolicy,
                                            stateFactory)

    queryContext.transactionalContext.transaction.freezeLocks()

    executionState.initializeState()
    workerManager.queryManager.addQuery(executingQuery)
    ProfiledQuerySubscription(executingQuery, queryProfile, stateFactory.memoryTracker)
  }
}
