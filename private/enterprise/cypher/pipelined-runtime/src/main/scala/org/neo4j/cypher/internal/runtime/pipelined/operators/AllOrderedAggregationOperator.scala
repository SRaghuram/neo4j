/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

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

class AllOrderedAggregationOperator(argumentStateMapId: ArgumentStateMapId,
                                    argumentSlotOffset: Int,
                                    val workIdentity: WorkIdentity,
                                    aggregations: Array[Aggregator],
                                    orderedGroupings: GroupingExpression,
                                    expressionValues: Array[Expression],
                                    outputSlots: Array[Int],
                                    argumentSize: SlotConfiguration.Size)
                                   (val id: Id = Id.INVALID_ID) extends Operator with OperatorState {

  private class AllOrderedAggregationState(var lastSeenGrouping: orderedGroupings.KeyType,
                                           val updaters: Array[Updater]) {
    def this() = this(null.asInstanceOf[orderedGroupings.KeyType], aggregations.map(_.newUpdater))
  }

  private var taskState: AllOrderedAggregationState = _

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    taskState = new AllOrderedAggregationState()
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new OptionalArgumentStateBuffer.Factory(stateFactory, id))
    this
  }

  override def toString: String = "AllOrderedAggregationOperator"

  override def nextTasks(state: PipelinedQueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    val input: MorselData = operatorInput.takeData()
    if (input != null) {
      IndexedSeq(new AllOrderedAggregationTask(input, taskState))
    } else {
      null
    }
  }

  class AllOrderedAggregationTask(morselData: MorselData,
                                  oangState: AllOrderedAggregationState) extends InputLoopWithMorselDataTask(morselData) {

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
      if (oangState.lastSeenGrouping != null && oangState.lastSeenGrouping != grouping) {
        writeRow(outputCursor, queryState)
      }
      oangState.lastSeenGrouping = grouping
      var i = 0
      while (i < aggregations.length) {
        val value = expressionValues(i)(inputCursor, queryState)
        oangState.updaters(i).update(value)
        i += 1
      }
    }

    override def processEndOfStream(outputCursor: MorselWriteCursor): Unit = {
      morselData.argumentStream match {
        case EndOfNonEmptyStream =>
          writeRow(outputCursor, queryState)
          oangState.lastSeenGrouping = null.asInstanceOf[orderedGroupings.KeyType]
        case _ =>
        // Do nothing
      }
    }

    private def writeRow(outputCursor: MorselWriteCursor, queryState: QueryState): Unit = {
      outputCursor.copyFrom(morselData.viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
      orderedGroupings.project(outputCursor, oangState.lastSeenGrouping)

      var i = 0
      while (i < aggregations.length) {
        // TODO use slotted aggregation functions?
        val reducer = aggregations(i).newStandardReducer(queryState.memoryTracker, id)
        reducer.update(oangState.updaters(i))
        outputCursor.setRefAt(outputSlots(i), reducer.result)
        oangState.updaters(i) = aggregations(i).newUpdater
        i += 1
      }
      outputCursor.next()
    }
  }
}
