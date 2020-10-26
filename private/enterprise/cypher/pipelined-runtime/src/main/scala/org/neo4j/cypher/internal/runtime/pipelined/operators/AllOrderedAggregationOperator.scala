/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
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
import org.neo4j.memory.MemoryTracker

class AllOrderedAggregationOperator(argumentStateMapId: ArgumentStateMapId,
                                    val workIdentity: WorkIdentity,
                                    aggregations: Array[AggregationExpression],
                                    orderedGroupings: GroupingExpression,
                                    outputSlots: Array[Int],
                                    argumentSize: SlotConfiguration.Size)
                                   (val id: Id = Id.INVALID_ID) extends Operator {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(
      argumentStateMapId,
      new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id),
      memoryTracker,
      ordered = true
    )
    new AllOrderedAggregationState(memoryTracker.getScopedMemoryTracker)
  }

  override def toString: String = "AllOrderedAggregationOperator"

  private class AllOrderedAggregationState(var lastSeenGrouping: orderedGroupings.KeyType,
                                           val aggregationFunctions: Array[AggregationFunction],
                                           val scopedMemoryTracker: MemoryTracker) extends DataInputOperatorState[MorselData] {
    def this(memoryTracker: MemoryTracker) =
      this(null.asInstanceOf[orderedGroupings.KeyType], aggregations.map(_.createAggregationFunction(memoryTracker)), memoryTracker)

    override def nextTasks(state: PipelinedQueryState,
                           input: MorselData,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorselData] =
      singletonIndexedSeq(new AllOrderedAggregationTask(input, this))
  }

  class AllOrderedAggregationTask(morselData: MorselData,
                                  taskState: AllOrderedAggregationState) extends InputLoopWithMorselDataTask(morselData) {

    override def workIdentity: WorkIdentity = AllOrderedAggregationOperator.this.workIdentity

    override def toString: String = "AllOrderedAggregationTask"

    private var queryState: QueryState = _

    override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = {
      queryState = state.queryStateForExpressionEvaluation(resources)
    }

    override def processRow(outputCursor: MorselWriteCursor,
                            inputCursor: MorselReadCursor): Unit = {
      val grouping = orderedGroupings.computeGroupingKey(inputCursor, queryState)
      // if new chunk
      if (taskState.lastSeenGrouping != null && taskState.lastSeenGrouping != grouping) {
        writeRow(outputCursor, queryState)
      }
      taskState.lastSeenGrouping = grouping
      var i = 0
      while (i < aggregations.length) {
        taskState.aggregationFunctions(i).apply(inputCursor, queryState)
        i += 1
      }
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit = {
      morselData.argumentStream match {
        case EndOfNonEmptyStream =>
          writeRow(outputCursor, queryState)
          taskState.lastSeenGrouping = null.asInstanceOf[orderedGroupings.KeyType]
        case _ =>
        // Do nothing
      }
    }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit = ()

    override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = ()

    private def writeRow(outputCursor: MorselWriteCursor, queryState: QueryState): Unit = {
      outputCursor.copyFrom(morselData.viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
      orderedGroupings.project(outputCursor, taskState.lastSeenGrouping)

      var i = 0
      val nAggregations = aggregations.length
      while (i < nAggregations) {
        outputCursor.setRefAt(outputSlots(i), taskState.aggregationFunctions(i).result(queryState))
        i += 1
      }
      taskState.scopedMemoryTracker.reset()
      i = 0
      while (i < nAggregations) {
        taskState.aggregationFunctions(i) = aggregations(i).createAggregationFunction(taskState.scopedMemoryTracker)
        i += 1
      }
      outputCursor.next()
    }
  }
}
