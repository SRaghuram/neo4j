/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import java.util.concurrent.ThreadFactory
import java.util.concurrent.locks.LockSupport

import org.neo4j.cypher.internal.physicalplanning.StateDefinition
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.internal.runtime.zombie.state.{ConcurrentStateFactory, TheExecutionState}
import org.neo4j.cypher.internal.runtime.zombie.{ExecutablePipeline, Worker}
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.values.AnyValue

/**
  * [[QueryExecutor]] implementation which uses a fixed number (n) of workers to execute
  * query work.
  */
class FixedWorkersQueryExecutor(morselSize: Int,
                                threadFactory: ThreadFactory,
                                val numberOfWorkers: Int,
                                transactionBinder: TransactionBinder,
                                queryResourceFactory: () => QueryResources)
  extends QueryExecutor
    with WorkerWaker
    with Lifecycle {

  private val queryManager = new QueryManager
  private val workers =
    (for (workerId <- 0 until numberOfWorkers) yield {
      new Worker(workerId, queryManager, LazyScheduling, queryResourceFactory())
    }).toArray

  // ========== LIFECYCLE ===========

  @volatile private var workerThreads: Array[Thread] = _

  override def init(): Unit = {}

  override def start(): Unit = {
    workerThreads = workers.map(threadFactory.newThread(_))
    workerThreads.foreach(_.start())
  }

  override def stop(): Unit = {
    workers.foreach(_.stop())
    workerThreads.foreach(_.interrupt())
    for (workerThread <- workerThreads) {
      workerThread.join(1000)
    }
  }

  override def shutdown(): Unit = {}

  // ========== WORKER WAKER ===========

  override def wakeAll(): Unit = {
    var i = 0
    while (i < workers.length) {
      LockSupport.unpark(workerThreads(i))
      i += 1
    }
  }

  // ========== QUERY EXECUTOR ===========

  override def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                                       stateDefinition: StateDefinition,
                                       inputDataStream: InputDataStream,
                                       queryContext: QueryContext,
                                       params: Array[AnyValue],
                                       schedulerTracer: SchedulerTracer,
                                       queryIndexes: Array[IndexReadSession],
                                       nExpressionSlots: Int,
                                       prePopulateResults: Boolean,
                                       subscriber: QuerySubscriber): QuerySubscription = {

    val tracer = schedulerTracer.traceQuery()
    val tracker = ConcurrentStateFactory.newTracker(subscriber, queryContext, tracer)
    val queryState = QueryState(params,
                                subscriber,
                                tracker,
                                null,
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
    val executionState = new TheExecutionState(stateDefinition,
                                               executablePipelines,
                                               ConcurrentStateFactory,
                                               this,
                                               queryContext,
                                               queryState,
                                               initResources,
                                               tracker)

    executionState.initializeState()

    val executingQuery = new ExecutingQuery(executionState,
                                            queryContext,
                                            queryState,
                                            tracer)

    queryManager.addQuery(executingQuery)
    tracker
  }
}
