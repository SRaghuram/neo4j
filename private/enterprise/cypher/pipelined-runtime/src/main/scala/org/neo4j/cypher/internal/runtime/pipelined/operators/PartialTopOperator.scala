/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Collections
import java.util.Comparator

import org.neo4j.cypher.internal.collection.DefaultComparatorTopTable
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.ReadableRow
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
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

class PartialTopWorkCanceller(override val argumentRowId: Long, limit: Int, memoryTracker: MemoryTracker) extends WorkCanceller {
  private var _remaining: Long = limit
  memoryTracker.allocateHeap(PartialTopWorkCanceller.SHALLOW_SIZE)

  override def isCancelled: Boolean = _remaining <= 0
  override def remaining: Long = _remaining
  def remaining_=(x: Long): Unit = _remaining = x

  override def argumentRowIdsForReducers: Array[Long] = null

  override def toString: String =
    s"PartialTopWorkCanceller(argumentRowId=$argumentRowId, remaining=$remaining)"

  override def shallowSize: Long = PartialTopWorkCanceller.SHALLOW_SIZE

  override def close(): Unit = {
    memoryTracker.releaseHeap(PartialTopWorkCanceller.SHALLOW_SIZE)
    super.close()
  }
}

object PartialTopWorkCanceller {
  private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[PartialTopWorkCanceller])

  class Factory(stateFactory: StateFactory, operatorId: Id, limit: Int) extends ArgumentStateFactory[PartialTopWorkCanceller] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): PartialTopWorkCanceller =
      new PartialTopWorkCanceller(argumentRowId, limit, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): PartialTopWorkCanceller =
      throw new IllegalStateException("PartialTop is not supported in parallel")
  }
}

class PartialTopOperator(bufferAsmId: ArgumentStateMapId,
                         workCancellerAsmId: ArgumentStateMapId,
                         argumentSlotOffset: Int,
                         val workIdentity: WorkIdentity,
                         prefixComparator: Comparator[ReadableRow],
                         suffixComparator: Comparator[ReadableRow],
                         limitExpression: Expression,
                         id: Id) extends Operator {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(bufferAsmId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), memoryTracker, ordered = true)
    val limit = Math.min(CountingState.evaluateCountValue(state, resources, limitExpression), ArrayUtil.MAX_ARRAY_SIZE).toInt
    val workCancellerAsm = argumentStateCreator.createArgumentStateMap(
      workCancellerAsmId,
      new PartialTopWorkCanceller.Factory(stateFactory, id, limit),
      memoryTracker
    )
    new PartialTopState(stateFactory.newMemoryTracker(id.x), limit, workCancellerAsm)
  }

  override def toString: String = "PartialTopOperator"

  private class PartialTopState(val memoryTracker: MemoryTracker,
                                val limit: Int,
                                val argumentStateMap: ArgumentStateMap[PartialTopWorkCanceller]) extends DataInputOperatorState[MorselData] {

    var remainingLimit: Int = limit
    var lastSeen: MorselRow = _
    var topTable: DefaultComparatorTopTable[MorselRow] = _
    var resultsIterator: util.Iterator[MorselRow] = Collections.emptyIterator()

    private var activeMemoryTracker: MemoryTracker = _
    private var resultsMemoryTracker: MemoryTracker = _

    override def nextTasks(state: PipelinedQueryState,
                           input: MorselData,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] =
      singletonIndexedSeq(new PartialTopTask(input, this))

    def addRow(row: MorselRow): Unit = {
      if (topTable == null) {
        activeMemoryTracker = memoryTracker.getScopedMemoryTracker
        topTable = new DefaultComparatorTopTable(suffixComparator, remainingLimit, activeMemoryTracker)
      }

      val evictedRow = topTable.addAndGetEvicted(row)
      if (row ne evictedRow) {
        activeMemoryTracker.allocateHeap(row.estimatedHeapUsage())
        if (evictedRow != null)
          activeMemoryTracker.releaseHeap(evictedRow.estimatedHeapUsage())
      }
      lastSeen = row
    }

    def computeResults(): Unit = {
      topTable.sort()
      resultsIterator = topTable.iterator().asInstanceOf[util.Iterator[MorselRow]]
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
