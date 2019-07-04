/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import java.util.concurrent.locks.LockSupport
import java.util.concurrent.{ThreadFactory, TimeUnit}

import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.morsel.state.{ConcurrentStateFactory, TheExecutionState}
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutablePipeline, WorkerManager}
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.values.AnyValue

import scala.concurrent.duration.Duration

/**
  * [[QueryExecutor]] implementation which uses a fixed number (n) of workers to execute
  * query work.
  */
class FixedWorkersQueryExecutor(morselSize: Int,
                                threadFactory: ThreadFactory,
                                numberOfWorkers: Int,
                                transactionBinder: TransactionBinder,
                                queryResourceFactory: () => QueryResources)
  extends WorkerManager(numberOfWorkers, new QueryManager, queryResourceFactory)
  with QueryExecutor
  with WorkerWaker
  with Lifecycle {

  // ========== LIFECYCLE ===========

  private val threadJoinWait = Duration(1, TimeUnit.MINUTES)

  @volatile private var workerThreads: Array[Thread] = _

  override def init(): Unit = {}

  override def start(): Unit = {
    DebugLog.log("starting worker threads")
    workers.foreach(_.reset())
    workerThreads = workers.map(threadFactory.newThread(_))
    workerThreads.foreach(_.start())
    DebugLog.logDiff("done")
  }

  override def stop(): Unit = {
    DebugLog.log("stopping worker threads")
    workers.foreach(_.stop())
    workerThreads.foreach(LockSupport.unpark)
    workerThreads.foreach(_.join(threadJoinWait.toMillis))
    workers.foreach(_.close())
    DebugLog.logDiff("done")
  }

  override def shutdown(): Unit = {}

  // ========== WORKER WAKER ===========

  override def wakeOne(): Unit = {
    var i = 0
    while (i < workers.length) {
      if (workers(i).isSleeping) {
        LockSupport.unpark(workerThreads(i))
        return
      }
      i += 1
    }
  }

  // ========== QUERY EXECUTOR ===========

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
                                       doProfile: Boolean): ProfiledQuerySubscription = {

    DebugLog.log("FixedWorkersQueryExecutor.execute()")

    val tracer = schedulerTracer.traceQuery()
    val tracker = ConcurrentStateFactory.newTracker(subscriber, queryContext, tracer)

    val queryState = QueryState(params,
                                subscriber,
                                tracker,
                                morselSize,
                                queryIndexes,
                                transactionBinder,
                                numberOfWorkers,
                                nExpressionSlots,
                                prePopulateResults,
                                inputDataStream)

    // instead of creating new resources here only for initializing the query, we could
    //    a) delegate initialization so some special init-pipeline/operator and use the regular worked resources
    //    b) attempt some lazy resource creation here, because they will usually not be needed
    val initResources = queryResourceFactory()
    val executionState = new TheExecutionState(executionGraphDefinition,
                                               executablePipelines,
                                               ConcurrentStateFactory,
                                               this,
                                               queryContext,
                                               queryState,
                                               initResources,
                                               tracker)

    val (workersProfiler, queryProfile) =
      if (doProfile) {
        val profiler = new FixedWorkersQueryProfiler(numberOfWorkers, executionGraphDefinition.applyRhsPlans)
        (profiler, profiler.Profile)
      } else {
        (WorkersQueryProfiler.NONE, QueryProfile.NONE)
      }

    val executingQuery = new ExecutingQuery(executionState,
                                            queryContext,
                                            queryState,
                                            tracer,
                                            workersProfiler)

    queryContext.transactionalContext.transaction.freezeLocks()

    queryManager.addQuery(executingQuery)
    executionState.initializeState()
    ProfiledQuerySubscription(tracker, queryProfile)
  }
}
