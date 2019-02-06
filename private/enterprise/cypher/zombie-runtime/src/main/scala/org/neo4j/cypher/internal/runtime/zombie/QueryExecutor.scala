/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.{InputDataStream, NoInput, QueryContext}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.result.QueryResult
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.MapValue

/**
  * Executor of queries. It's currently a merge of a dispatcher, a scheduler and a spatula.
  */
trait QueryExecutor {
  def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                              state: ExecutionState,
                              inputDataStream: InputDataStream,
                              queryContext: QueryContext,
                              params: MapValue,
                              schedulerTracer: SchedulerTracer,
                              queryIndexes: Array[IndexReadSession],
                              visitor: QueryResult.QueryResultVisitor[E]): Unit
}

class SingleThreadedQueryExecutor(transactionBinder: TransactionBinder) extends QueryExecutor {
  override def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                                       executionState: ExecutionState,
                                       inputDataStream: InputDataStream,
                                       queryContext: QueryContext,
                                       params: MapValue,
                                       schedulerTracer: SchedulerTracer,
                                       queryIndexes: Array[IndexReadSession],
                                       visitor: QueryResult.QueryResultVisitor[E]): Unit = {

    val resources = new QueryResources(queryContext.transactionalContext.cursors)
    val state = QueryState(params,
                           visitor,
                           4,
                           queryIndexes,
                           transactionBinder,
                           1,
                           inputDataStream)

    executionState.initialize()

    val reversedPipelines = executablePipelines.reverse
    while (executeNextWorkUnit(reversedPipelines, executionState, queryContext, resources, state)) {}
  }

  private def executeNextWorkUnit(executablePipelines: IndexedSeq[ExecutablePipeline],
                                  executionState: ExecutionState,
                                  queryContext: QueryContext,
                                  resources: QueryResources,
                                  state: QueryState): Boolean = {

    val task = findNextTask(executablePipelines, executionState, queryContext, resources, state)
    if (task != null) {
      val pipeline = task.pipeline
      val slots = pipeline.slots
      val slotSize = slots.size()
      val morsel = Morsel.create(slots, state.morselSize)
      val output = new MorselExecutionContext(morsel,
                                              slotSize.nLongs,
                                              slotSize.nReferences,
                                              state.morselSize,
                                              currentRow = 0,
                                              slots)

      task.executeWorkUnit(resources, output)

      if (pipeline.output != null ) {
        output.resetToFirstRow()
        executionState.produceMorsel(pipeline.output.id, output)
      }

      if (task.canContinue) {
        executionState.addContinuation(task)
      } else {
        executionState.closeMorsel(pipeline.inputRowBuffer.id, task.start.inputMorsel)
      }
      true
    } else false
  }

  /**
    * Find the next task to execute. This will eventually be abstracted into a scheduling policy.
    */
  private def findNextTask(executablePipelines: IndexedSeq[ExecutablePipeline],
                           executionState: ExecutionState,
                           queryContext: QueryContext,
                           resources: QueryResources,
                           state: QueryState): PipelineTask = {

    for (p <- executablePipelines) {
      val task = executionState.continue(p)
      if (task != null) {
        return task
      }

      val input = executionState.consumeMorsel(p.inputRowBuffer.id)
      if (input != null) {
        val pipelineState = executionState.pipelineState(p.id)
        val tasks = pipelineState.init(input, queryContext, state, resources)
        for (task <- tasks.tail)
          executionState.addContinuation(task)
        return tasks.head
      }
    }
    null
  }
}
