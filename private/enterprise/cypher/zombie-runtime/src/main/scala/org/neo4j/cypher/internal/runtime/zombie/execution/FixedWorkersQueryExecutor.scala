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
import org.neo4j.cypher.internal.runtime.zombie.state.{ConcurrentStateFactory, PipelineExecutions, TheExecutionState}
import org.neo4j.cypher.internal.runtime.zombie.{ExecutablePipeline, Worker}
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue

/**
  * [[QueryExecutor]] implementation which uses a fixed number (n) of workers to execute
  * query work.
  */
class FixedWorkersQueryExecutor(morselSize: Int,
                                threadFactory: ThreadFactory,
                                val numberOfWorkers: Int,
                                transactionBinder: TransactionBinder,
                                queryResourceFactory: () => QueryResources) extends QueryExecutor with WorkerWaker {

  private val queryManager = new QueryManager
  private val workers =
    (for (workerId <- 0 until numberOfWorkers) yield {
      new Worker(workerId, queryManager, LazyScheduling, queryResourceFactory())
    }).toArray
  private val workerThreads = workers.map(threadFactory.newThread(_))

  /**
    * Start all worker threads
    */
  // TODO: shouldn't be done in constructor: integrate with lifecycle properly instead
  workerThreads.foreach(_.start())

  override def wakeAll(): Unit = {
      var i = 0
    while (i < workers.length) {
      LockSupport.unpark(workerThreads(i))
      i += 1
    }
  }

  override def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                                       stateDefinition: StateDefinition,
                                       inputDataStream: InputDataStream,
                                       queryContext: QueryContext,
                                       params: Array[AnyValue],
                                       schedulerTracer: SchedulerTracer,
                                       queryIndexes: Array[IndexReadSession],
                                       nExpressionSlots: Int,
                                       visitor: QueryResult.QueryResultVisitor[E]): QueryExecutionHandle = {

    val executionState = TheExecutionState.build(stateDefinition, executablePipelines, ConcurrentStateFactory, this)

    val queryState = QueryState(params,
                                visitor,
                                morselSize,
                                queryIndexes,
                                transactionBinder,
                                numberOfWorkers = 1,
                                nExpressionSlots,
                                inputDataStream)

    // instead of creating new resources here only for initializing the query, we could
    //    a) delegate initialization so some special init-pipeline/operator and use the regular worked resources
    //    b) attempt some lazy resource creation here, because they will usually not be needed
    val initResources = new QueryResources(queryContext.transactionalContext.cursors)
    val pipelineExecutions = new PipelineExecutions(executablePipelines,
                                                    executionState,
                                                    queryContext,
                                                    queryState,
                                                    initResources)

    executionState.initialize()

    val executingQuery = new ExecutingQuery(pipelineExecutions,
                                            executionState,
                                            queryContext,
                                            queryState,
                                            schedulerTracer.traceQuery())

    queryManager.addQuery(executingQuery)
    executingQuery
  }
}
