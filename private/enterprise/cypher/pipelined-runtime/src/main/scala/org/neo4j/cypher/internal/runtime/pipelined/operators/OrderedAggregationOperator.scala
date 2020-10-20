/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Map

import org.eclipse.collections.api.block.function.Function2
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.computeNewAggregatorsFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap
import org.neo4j.memory.MemoryTracker

class OrderedAggregationOperator(argumentStateMapId: ArgumentStateMapId,
                                 argumentSlotOffset: Int,
                                 val workIdentity: WorkIdentity,
                                 aggregations: Array[AggregationExpression],
                                 orderedGroupings: GroupingExpression,
                                 unorderedGroupings: GroupingExpression,
                                 outputSlots: Array[Int],
                                 argumentSize: SlotConfiguration.Size)
                                (val id: Id = Id.INVALID_ID) extends Operator {

  private type ResultsMap = HeapTrackingOrderedAppendMap[unorderedGroupings.KeyType, Array[AggregationFunction]]
  private type ResultsIterator = util.Iterator[Map.Entry[unorderedGroupings.KeyType, Array[AggregationFunction]]]
  private case class Result(orderedGroupingKey: orderedGroupings.KeyType, resultsIterator: ResultsIterator)

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {

    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    new OrderedAggregationState(memoryTracker)
  }

  override def toString: String = "OrderedAggregationOperator"

  private val newAggregationFunctions: Function2[unorderedGroupings.KeyType, MemoryTracker, Array[AggregationFunction]] =
    computeNewAggregatorsFunction(aggregations)

  private class OrderedAggregationState(var lastSeenGroupingKey: orderedGroupings.KeyType,
                                        var resultsMap: ResultsMap,
                                        var outstandingResults: Result,
                                        val memoryTracker: MemoryTracker) extends DataInputOperatorState[MorselData] {
    def this(memoryTracker: MemoryTracker) =
      this(null.asInstanceOf[orderedGroupings.KeyType], HeapTrackingOrderedAppendMap.createOrderedMap(memoryTracker), null, memoryTracker)

    override def nextTasks(state: PipelinedQueryState,
                           input: MorselData,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] =
      singletonIndexedSeq(new OrderedAggregationTask(input, this))
  }

  class OrderedAggregationTask(morselData: MorselData,
                               taskState: OrderedAggregationState) extends InputLoopWithMorselDataTask(morselData) {

    override def workIdentity: WorkIdentity = OrderedAggregationOperator.this.workIdentity

    override def toString: String = "OrderedAggregationTask"

    private var queryState: QueryState = _

    override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = {
      queryState = state.queryStateForExpressionEvaluation(resources)
    }

    override def processRow(outputCursor: MorselWriteCursor,
                            inputCursor: MorselReadCursor): Unit = {
      val orderedGroupingKey = orderedGroupings.computeGroupingKey(inputCursor, queryState)
      // if new chunk
      if (taskState.lastSeenGroupingKey != null && taskState.lastSeenGroupingKey != orderedGroupingKey) {
        completeCurrentChunk()
        tryWriteOutstandingResults(outputCursor, queryState)
      }
      taskState.lastSeenGroupingKey = orderedGroupingKey

      val unorderedGroupingKey = unorderedGroupings.computeGroupingKey(inputCursor, queryState)
      val aggFunctions = taskState.resultsMap.getIfAbsentPutWithMemoryTracker2(unorderedGroupingKey, newAggregationFunctions)

      var i = 0
      while (i < aggFunctions.length) {
        aggFunctions(i).apply(inputCursor, queryState)
        i += 1
      }
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit =
      morselData.argumentStream match {
        case EndOfNonEmptyStream =>
          if (taskState.lastSeenGroupingKey != null)
            completeCurrentChunk()
        case _ =>
        // Do nothing
      }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit =
      tryWriteOutstandingResults(outputCursor, queryState)

    override def canContinue: Boolean = super.canContinue || taskState.outstandingResults != null

    override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = ()

    private def tryWriteOutstandingResults(outputCursor: MorselWriteCursor, queryState: QueryState): Unit = {
      while (taskState.outstandingResults != null && outputCursor.onValidRow()) {
        val result = taskState.outstandingResults
        val it = result.resultsIterator
        if (!it.hasNext) {
          taskState.outstandingResults = null
        } else {
          val entry = it.next() // NOTE: This entry is transient and only valid until we call next() again
          val unorderedGroupingKey = entry.getKey
          val aggResults = entry.getValue
          outputCursor.copyFrom(morselData.viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
          orderedGroupings.project(outputCursor, result.orderedGroupingKey)
          unorderedGroupings.project(outputCursor, unorderedGroupingKey)

          var i = 0
          while (i < aggregations.length) {
            outputCursor.setRefAt(outputSlots(i), aggResults(i).result(queryState))
            i += 1
          }
          outputCursor.next()
        }
      }
    }

    private def completeCurrentChunk(): Unit = {
      val outstandingResultsIterator = taskState.resultsMap.autoClosingEntryIterator()
      taskState.outstandingResults = Result(taskState.lastSeenGroupingKey, outstandingResultsIterator)
      taskState.lastSeenGroupingKey = null.asInstanceOf[orderedGroupings.KeyType]
      taskState.resultsMap = HeapTrackingOrderedAppendMap.createOrderedMap(taskState.memoryTracker)
    }
  }
}
