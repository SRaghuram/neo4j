/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import org.neo4j.cypher.internal.physicalplanning.StateDefinition
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.internal.runtime.zombie.state.{PipelineExecutions, StandardStateFactory, TheExecutionState}
import org.neo4j.cypher.internal.runtime.zombie.{ExecutablePipeline, Worker}
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue

/**
  * Single threaded implementation of [[QueryExecutor]]. Executes the query on
  * the thread which calls execute, without any synchronization with other queries
  * or any parallel execution.
  */
class CallingThreadQueryExecutor(morselSize: Int, transactionBinder: TransactionBinder) extends QueryExecutor with WorkerWaker {

  override def wakeAll(): Unit = ()

  override def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                                       stateDefinition: StateDefinition,
                                       inputDataStream: InputDataStream,
                                       queryContext: QueryContext,
                                       params: Array[AnyValue],
                                       schedulerTracer: SchedulerTracer,
                                       queryIndexes: Array[IndexReadSession],
                                       nExpressionSlots: Int,
                                       visitor: QueryResult.QueryResultVisitor[E]): QueryExecutionHandle = {

    val resources = new QueryResources(queryContext.transactionalContext.cursors)
    val queryState = QueryState(params,
                                visitor,
                                morselSize,
                                queryIndexes,
                                transactionBinder,
                                numberOfWorkers = 1,
                                nExpressionSlots,
                                inputDataStream)

    val executionState = TheExecutionState.build(stateDefinition, executablePipelines, StandardStateFactory, this)
    val pipelineExecutions = new PipelineExecutions(executablePipelines, executionState, queryContext, queryState, resources)

    executionState.initialize()

    val worker = new Worker(1, null, LazyScheduling, resources)
    val executingQuery = new ExecutingQuery(pipelineExecutions, executionState, queryContext, queryState, schedulerTracer.traceQuery())
    // TODO: currently busy looping until all work is done... this is a bad
    //       way to handle back-pressure with reactive results
    while (worker.workOnQuery(executingQuery)) {}
    executingQuery
  }
}
