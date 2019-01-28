package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.{Rows, Sink}
import org.neo4j.cypher.internal.runtime.{InputDataStream, NoInput, QueryContext}
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
                                       newQueryState: ExecutionState,
                                       inputDataStream: InputDataStream,
                                       queryContext: QueryContext,
                                       params: MapValue,
                                       schedulerTracer: SchedulerTracer,
                                       queryIndexes: Array[IndexReadSession],
                                       visitor: QueryResult.QueryResultVisitor[E]): Unit = {

    val resources = new QueryResources(queryContext.transactionalContext.cursors)
    val state = QueryState(params,
                           visitor,
                           1000,
                           queryIndexes,
                           transactionBinder,
                           1,
                           inputDataStream)

    newQueryState.initialize()

    val reversedPipelines = executablePipelines.reverse
    while (executeNextWorkUnit(reversedPipelines, newQueryState, queryContext, resources, state)) {}
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
      pipeline.output match {
        case Rows(outputId, _) =>
          output.resetToFirstRow()
          executionState.produceStreamingRows(outputId, output)
        case Sink =>
      }
      if (task.canContinue) {
        executionState.addContinuation(task)
      }
      true
    } else false
  }

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

      val input = executionState.consumeStreamingRows(p.lhsRows.id)
      if (input != null) {
        val tasks = p.init(input, queryContext, state, resources)
        for (task <- tasks.tail)
          executionState.addContinuation(task)
        return tasks.head
      }
    }
    null
  }
}
