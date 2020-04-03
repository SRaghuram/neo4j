/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Updater
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.OptionalArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

class OrderedAggregationOperator(argumentStateMapId: ArgumentStateMapId,
                                 argumentSlotOffset: Int,
                                 val workIdentity: WorkIdentity,
                                 aggregations: Array[Aggregator],
                                 orderedGroupings: GroupingExpression,
                                 unorderedGroupings: GroupingExpression,
                                 expressionValues: Array[Expression],
                                 outputSlots: Array[Int],
                                 argumentSize: SlotConfiguration.Size)
                                (val id: Id = Id.INVALID_ID) extends Operator with OperatorState {

  private type UpdatersMap = util.LinkedHashMap[unorderedGroupings.KeyType, Array[Updater]]
  private case class Chunk(orderedGroupingKey: orderedGroupings.KeyType, updatersMap: UpdatersMap)
  private type ChunkList = util.LinkedList[Chunk]

  private class OrderedAggregationState(var lastSeenGroupingKey: orderedGroupings.KeyType,
                                        var updatersMap: UpdatersMap,
                                        var chunks: ChunkList) {
    def this() = this(null.asInstanceOf[orderedGroupings.KeyType], new UpdatersMap, new ChunkList)
  }

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    taskState = new OrderedAggregationState()
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new OptionalArgumentStateBuffer.Factory(stateFactory, id))
    this
  }

  override def toString: String = "OrderedAggregationOperator"

  override def nextTasks(state: PipelinedQueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    val input: MorselData = operatorInput.takeData()
    if (input != null) {
      IndexedSeq(new OrderedAggregationTask(input, taskState))
    } else {
      null
    }
  }

  private var taskState: OrderedAggregationState = _

  private val newUpdaters: util.function.Function[unorderedGroupings.KeyType, Array[Updater]] =
    (_: unorderedGroupings.KeyType) => aggregations.map(_.newUpdater)

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
      val updaters = taskState.updatersMap.computeIfAbsent(unorderedGroupingKey, newUpdaters)

      var i = 0
      while (i < aggregations.length) {
        val value = expressionValues(i)(inputCursor, queryState)
        updaters(i).update(value)
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

    override def canContinue: Boolean = super.canContinue || !taskState.chunks.isEmpty

    private def tryWriteOutstandingResults(outputCursor: MorselWriteCursor, queryState: QueryState): Unit = {
      while (!taskState.chunks.isEmpty && outputCursor.onValidRow()) {
        val chunk = taskState.chunks.getFirst
        val it = chunk.updatersMap.entrySet().iterator()
        if (!it.hasNext) {
          taskState.chunks.removeFirst()
        } else {
          val entry = it.next()
          val unorderedColumns = entry.getKey
          val updaters = entry.getValue
          outputCursor.copyFrom(morselData.viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
          orderedGroupings.project(outputCursor, chunk.orderedGroupingKey)
          unorderedGroupings.project(outputCursor, unorderedColumns)

          var i = 0
          while (i < aggregations.length) {
            // TODO use slotted aggregation functions?
            val reducer = aggregations(i).newStandardReducer(queryState.memoryTracker, id)
            reducer.update(updaters(i))
            outputCursor.setRefAt(outputSlots(i), reducer.result)
            i += 1
          }
          it.remove()
          outputCursor.next()
        }
      }
    }

    private def completeCurrentChunk(): Unit = {
      taskState.chunks.add(Chunk(taskState.lastSeenGroupingKey, taskState.updatersMap))
      taskState.lastSeenGroupingKey = null.asInstanceOf[orderedGroupings.KeyType]
      taskState.updatersMap = new UpdatersMap
    }
  }
}
