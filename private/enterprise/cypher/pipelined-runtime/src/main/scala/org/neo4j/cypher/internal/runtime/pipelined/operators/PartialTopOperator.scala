/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Collections
import java.util.Comparator

import org.neo4j.cypher.internal.DefaultComparatorTopTable
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.helpers.ArrayUtil

class PartialTopWorkCanceller(override val argumentRowId: Long, limit: Int) extends WorkCanceller {

  private var _remaining: Long = limit

  override def isCancelled: Boolean = _remaining <= 0
  override def remaining: Long = _remaining
  def remaining_=(x: Long): Unit = _remaining = x

  override def argumentRowIdsForReducers: Array[Long] = null

  override def toString: String =
    s"PartialTopWorkCanceller(argumentRowId=$argumentRowId, remaining=$remaining)"
}

object PartialTopWorkCanceller {
  class Factory(stateFactory: StateFactory, operatorId: Id, limit: Int) extends ArgumentStateFactory[PartialTopWorkCanceller] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): PartialTopWorkCanceller =
      new PartialTopWorkCanceller(argumentRowId, limit)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): PartialTopWorkCanceller =
      throw new IllegalStateException("PartialTop is not supported in parallel")
  }
}

class PartialTopOperator(bufferAsmId: ArgumentStateMapId,
                         workCancellerAsmId: ArgumentStateMapId,
                         argumentSlotOffset: Int,
                         val workIdentity: WorkIdentity,
                         prefixComparator: Comparator[MorselRow],
                         suffixComparator: Comparator[MorselRow],
                         limitExpression: Expression,
                         id: Id) extends Operator {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(bufferAsmId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    val limit = Math.min(CountingState.evaluateCountValue(state, resources, limitExpression), ArrayUtil.MAX_ARRAY_SIZE).toInt
    val workCancellerAsm = argumentStateCreator.createArgumentStateMap(workCancellerAsmId, new PartialTopWorkCanceller.Factory(stateFactory, id, limit), ordered = false)
    new PartialTopState(stateFactory.memoryTracker, limit, workCancellerAsm)
  }

  override def toString: String = "PartialTopOperator"

  private class PartialTopState(val memoryTracker: QueryMemoryTracker,
                                val limit: Int,
                                val argumentStateMap: ArgumentStateMap[PartialTopWorkCanceller],
                                var lastSeen: MorselRow,
                                var topTable: DefaultComparatorTopTable[MorselRow],
                                var resultsIterator: util.Iterator[MorselRow]) extends OperatorState {

    var remainingLimit: Int = limit

    def this(memoryTracker: QueryMemoryTracker, limit: Int, argumentStateMap: ArgumentStateMap[PartialTopWorkCanceller]) =
      this(memoryTracker, limit, argumentStateMap, null, new DefaultComparatorTopTable[MorselRow](suffixComparator, limit), Collections.emptyIterator())

    override def nextTasks(state: PipelinedQueryState,
                           operatorInput: OperatorInput,
                           parallelism: Int,
                           resources: QueryResources,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
      val input: MorselData = operatorInput.takeData()
      if (input != null) singletonIndexedSeq(new PartialTopTask(input, this))
      else null
    }
  }

  class PartialTopTask(morselData: MorselData,
                       taskState: PartialTopState) extends InputLoopWithMorselDataTask(morselData) {

    override def workIdentity: WorkIdentity = PartialTopOperator.this.workIdentity

    override def toString: String = "PartialTopTask"

    override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = ()

    override def processRow(outputCursor: MorselWriteCursor,
                            inputCursor: MorselReadCursor): Unit = {
      if (taskState.remainingLimit > 0) {
        // if new chunk
        if (taskState.lastSeen != null && prefixComparator.compare(taskState.lastSeen, inputCursor) != 0) {
          completeCurrentChunk(taskState.remainingLimit - taskState.topTable.getSize)
          tryWriteOutstandingResults(outputCursor)
        }

        // if we have reached the end of a chunk, remaining limit might've changed, check again
        if (taskState.remainingLimit > 0) {
          taskState.lastSeen = inputCursor.snapshot()
          val sizeBefore = taskState.topTable.getSize
          taskState.topTable.add(taskState.lastSeen)
          val sizeAfter = taskState.topTable.getSize
          if (sizeAfter > sizeBefore)
            taskState.memoryTracker.allocated(taskState.lastSeen, id.x)
        }
      }
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit = {
      morselData.argumentStream match {
        case EndOfNonEmptyStream =>
          if (taskState.lastSeen != null)
            completeCurrentChunk(0)

          // reset limit for the next argument id
          taskState.remainingLimit = taskState.limit
          taskState.topTable = new DefaultComparatorTopTable[MorselRow](suffixComparator, taskState.limit)
        case _ =>
        // Do nothing
      }
    }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit =
      tryWriteOutstandingResults(outputCursor)

    override def canContinue: Boolean = super.canContinue || taskState.resultsIterator.hasNext

    private def tryWriteOutstandingResults(outputCursor: MorselWriteCursor): Unit = {
      while (taskState.resultsIterator.hasNext && outputCursor.onValidRow()) {
        val morselRow = taskState.resultsIterator.next()
        outputCursor.copyFrom(morselRow)
        taskState.memoryTracker.deallocated(morselRow, id.x)
        outputCursor.next()
      }
    }

    private def completeCurrentChunk(nextRemainingLimit: Int): Unit = {
      val argumentRowId = ArgumentSlots.getArgumentAt(taskState.lastSeen, argumentSlotOffset)
      taskState.argumentStateMap.update(argumentRowId, _.remaining = nextRemainingLimit)

      taskState.topTable.sort()
      taskState.resultsIterator = taskState.topTable.iterator()

      taskState.lastSeen = null
      taskState.remainingLimit = nextRemainingLimit

      if (nextRemainingLimit > 0)
        taskState.topTable = new DefaultComparatorTopTable[MorselRow](suffixComparator, nextRemainingLimit)
      else
        taskState.topTable = null
    }
  }
}
