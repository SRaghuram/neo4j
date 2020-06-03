/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.Comparator

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

class PartialSortOperator(val argumentStateMapId: ArgumentStateMapId,
                          val workIdentity: WorkIdentity,
                          prefixComparator: Comparator[ReadableRow],
                          suffixComparator: Comparator[ReadableRow])
                         (val id: Id = Id.INVALID_ID) extends Operator {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    new PartialSortState(stateFactory.newMemoryTracker(id.x))
  }

  override def toString: String = "PartialSortOperator"

  private class PartialSortState(val memoryTracker: MemoryTracker) extends DataInputOperatorState[MorselData] {
    var lastSeen: MorselRow = _
    val resultsBuffer: HeapTrackingArrayList[MorselRow] = HeapTrackingArrayList.newArrayList(16, memoryTracker)
    var remainingResultsIndex: Int = -1
    var currentMorselHeapUsage: Long = 0

    // Memory for morsels is released in one go after all output rows for completed chunk have been written
    val morselMemoryTracker: MemoryTracker = memoryTracker.getScopedMemoryTracker

    override def nextTasks(input: MorselData): IndexedSeq[ContinuableOperatorTask] = singletonIndexedSeq(new PartialSortTask(input, this))
  }

  class PartialSortTask(morselData: MorselData,
                        taskState: PartialSortState) extends InputLoopWithMorselDataTask(morselData) {

    override def workIdentity: WorkIdentity = PartialSortOperator.this.workIdentity

    override def toString: String = "PartialSortTask"

    override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = ()

    override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = {
      val heapUsage = inputCursor.morsel.estimatedHeapUsage
      taskState.currentMorselHeapUsage = heapUsage
      taskState.morselMemoryTracker.allocateHeap(heapUsage)
    }

    override def processRow(outputCursor: MorselWriteCursor,
                            inputCursor: MorselReadCursor): Unit = {
      // if new chunk
      if (taskState.lastSeen != null && prefixComparator.compare(taskState.lastSeen, inputCursor) != 0) {
        completeCurrentChunk()
        tryWriteOutstandingResults(outputCursor)
      }
      taskState.lastSeen = inputCursor.snapshot()
      if (taskState.remainingResultsIndex == -1)
        taskState.resultsBuffer.add(taskState.lastSeen)
      taskState.memoryTracker.allocateHeap(taskState.lastSeen.shallowInstanceHeapUsage)
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit =
      morselData.argumentStream match {
        case EndOfNonEmptyStream =>
          taskState.currentMorselHeapUsage = 0
          if (taskState.lastSeen != null)
            completeCurrentChunk()
        case _ =>
        // Do nothing
      }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit =
      tryWriteOutstandingResults(outputCursor)

    override def canContinue: Boolean = super.canContinue || taskState.remainingResultsIndex >= 0

    private def tryWriteOutstandingResults(outputCursor: MorselWriteCursor): Unit = {
      while (taskState.remainingResultsIndex >= 0 && outputCursor.onValidRow()) {
        val idx = taskState.remainingResultsIndex
        val buf = taskState.resultsBuffer
        if (idx >= buf.size()) {
          buf.clear()
          taskState.remainingResultsIndex = -1
          if (taskState.lastSeen != null)
            buf.add(taskState.lastSeen)

          // current morsel might contain more chunks, so we don't know if it should be released just yet
          taskState.morselMemoryTracker.reset()
          if (taskState.currentMorselHeapUsage > 0)
            taskState.morselMemoryTracker.allocateHeap(taskState.currentMorselHeapUsage)
        } else {
          val morselRow = buf.get(idx)
          outputCursor.copyFrom(morselRow)

          taskState.memoryTracker.releaseHeap(morselRow.shallowInstanceHeapUsage)
          buf.set(idx, null)
          taskState.remainingResultsIndex += 1
          outputCursor.next()
        }
      }
    }

    private def completeCurrentChunk(): Unit = {
      val buffer = taskState.resultsBuffer

      if (buffer.size() > 1) {
        buffer.sort(suffixComparator)
      }

      taskState.remainingResultsIndex = 0
      taskState.lastSeen = null
    }
  }
}
