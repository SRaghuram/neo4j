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
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.ScopedMemoryTracker

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
    val workCancellerAsm = argumentStateCreator.createArgumentStateMap(workCancellerAsmId, new PartialTopWorkCanceller.Factory(stateFactory, id, limit))
    new PartialTopState(stateFactory.newMemoryTracker(id.x), limit, workCancellerAsm)
  }

  override def toString: String = "PartialTopOperator"

  private class PartialTopState(val memoryTracker: MemoryTracker,
                                val limit: Int,
                                val argumentStateMap: ArgumentStateMap[PartialTopWorkCanceller]) extends OperatorState {

    var remainingLimit: Int = limit
    var lastSeen: MorselRow = _
    var topTable: DefaultComparatorTopTable[MorselRow] = _
    var resultsIterator: util.Iterator[MorselRow] = Collections.emptyIterator()

    private var activeMemoryTracker: ScopedMemoryTracker = _
    private var resultsMemoryTracker: ScopedMemoryTracker = _

    override def nextTasks(state: PipelinedQueryState,
                           operatorInput: OperatorInput,
                           parallelism: Int,
                           resources: QueryResources,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
      val input: MorselData = operatorInput.takeData()
      if (input != null) singletonIndexedSeq(new PartialTopTask(input, this))
      else null
    }

    def addRow(row: MorselRow): Unit = {
      if (topTable == null) {
        activeMemoryTracker = new ScopedMemoryTracker(memoryTracker)
        topTable = new DefaultComparatorTopTable[MorselRow](suffixComparator, remainingLimit, activeMemoryTracker)
      }

      lastSeen = row
      val sizeBefore = topTable.getSize
      topTable.add(lastSeen)
      val sizeAfter = topTable.getSize
      if (sizeAfter > sizeBefore)
        activeMemoryTracker.allocateHeap(lastSeen.estimatedHeapUsage)
    }

    def computeResults(): Unit = {
      topTable.sort()
      resultsIterator = topTable.iterator()
      resultsMemoryTracker = activeMemoryTracker

      topTable = null
      activeMemoryTracker = null
    }

    def clearResults(): Unit = {
      if (resultsMemoryTracker != null) {
        resultsIterator = Collections.emptyIterator()
        resultsMemoryTracker.close()
        resultsMemoryTracker = null
      }
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
        if (taskState.remainingLimit > 0)
          taskState.addRow(inputCursor.snapshot())
      }
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit = {
      morselData.argumentStream match {
        case EndOfNonEmptyStream =>
          if (taskState.lastSeen != null)
            completeCurrentChunk(0)

          // reset limit for the next argument id
          taskState.remainingLimit = taskState.limit
        case _ =>
        // Do nothing
      }
    }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit =
      tryWriteOutstandingResults(outputCursor)

    override def canContinue: Boolean = super.canContinue || taskState.resultsIterator.hasNext

    override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = ()

    private def tryWriteOutstandingResults(outputCursor: MorselWriteCursor): Unit = {
      while (taskState.resultsIterator.hasNext && outputCursor.onValidRow()) {
        val morselRow = taskState.resultsIterator.next()
        outputCursor.copyFrom(morselRow)
        outputCursor.next()
      }

      if (!taskState.resultsIterator.hasNext)
        taskState.clearResults()
    }

    private def completeCurrentChunk(nextRemainingLimit: Int): Unit = {
      val argumentRowId = ArgumentSlots.getArgumentAt(taskState.lastSeen, argumentSlotOffset)
      taskState.argumentStateMap.update(argumentRowId, _.remaining = nextRemainingLimit)

      taskState.computeResults()

      taskState.lastSeen = null
      taskState.remainingLimit = nextRemainingLimit
    }
  }
}
