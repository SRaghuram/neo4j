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
    new PartialSortState(stateFactory.newMemoryTracker(id.x), prefixComparator, suffixComparator, workIdentity)
  }

  override def toString: String = "PartialSortOperator"
}

class PartialSortTask(morselData: MorselData,
                      override val workIdentity: WorkIdentity,
                      taskState: PartialSortState) extends InputLoopWithMorselDataTask(morselData) {

  override def toString: String = "PartialSortTask"

  override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = ()

  override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit =
    taskState.onNewInputMorsel(inputCursor)

  override def processRow(outputCursor: MorselWriteCursor,
                          inputCursor: MorselReadCursor): Unit =
    taskState.onRow(outputCursor, inputCursor)

  override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit =
    morselData.argumentStream match {
      case EndOfNonEmptyStream =>
        taskState.onEndOfStream()
      case _ =>
      // Do nothing
    }

  override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit =
    taskState.tryWriteOutstandingResults(outputCursor)

  override def canContinue: Boolean = super.canContinue || taskState.hasOutputToWrite
}

class PartialSortState(memoryTracker: MemoryTracker,
                       prefixComparator: Comparator[ReadableRow],
                       suffixComparator: Comparator[ReadableRow],
                       workIdentity: WorkIdentity) extends DataInputOperatorState[MorselData] {

  private[this] val buffer: HeapTrackingArrayList[MorselRow] = HeapTrackingArrayList.newArrayList(16, memoryTracker)
  private[this] var lastSeen: MorselRow = _
  private[this] var outputIndex: Int = -1

  private[this] var currentMorselHeapUsage: Long = 0
  // Memory for morsels is released in one go after all output rows for completed chunk have been written
  private[this] val morselMemoryTracker: MemoryTracker = memoryTracker.getScopedMemoryTracker

  def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = {
    val heapUsage = inputCursor.morsel.estimatedHeapUsage
    currentMorselHeapUsage = heapUsage
    morselMemoryTracker.allocateHeap(heapUsage)
  }

  def onRow(outputCursor: MorselWriteCursor,
            inputCursor: MorselReadCursor): Unit = {
    // if new chunk
    if (lastSeen != null && prefixComparator.compare(lastSeen, inputCursor) != 0) {
      completeCurrentChunk()
      tryWriteOutstandingResults(outputCursor)
    }

    lastSeen = inputCursor.snapshot()
    memoryTracker.allocateHeap(lastSeen.shallowInstanceHeapUsage)

    if (!hasOutputToWrite) // if there _is_ output, add it after we're done writing
      buffer.add(lastSeen)
  }

  def onEndOfStream(): Unit = {
    currentMorselHeapUsage = 0
    if (lastSeen != null)
      completeCurrentChunk()
  }

  def tryWriteOutstandingResults(outputCursor: MorselWriteCursor): Unit = {
    while (outputIndex >= 0 && outputCursor.onValidRow()) {
      if (outputIndex >= buffer.size()) {
        resetBuffer()
        outputIndex = -1
      } else {
        val morselRow = buffer.get(outputIndex)
        outputCursor.copyFrom(morselRow)

        memoryTracker.releaseHeap(morselRow.shallowInstanceHeapUsage)
        buffer.set(outputIndex, null)
        outputIndex += 1
        outputCursor.next()
      }
    }
  }

  def hasOutputToWrite: Boolean = outputIndex >= 0

  private def completeCurrentChunk(): Unit = {
    if (buffer.size() > 1)
      buffer.sort(suffixComparator)

    outputIndex = 0
    lastSeen = null
  }

  private def resetBuffer(): Unit = {
    buffer.clear()

    // add pending row from next chunk
    if (lastSeen != null)
      buffer.add(lastSeen)

    // current morsel might contain more chunks, so we don't know if it should be released just yet
    morselMemoryTracker.reset()
    if (currentMorselHeapUsage > 0)
      morselMemoryTracker.allocateHeap(currentMorselHeapUsage)
  }

  override def nextTasks(input: MorselData): IndexedSeq[ContinuableOperatorTask] =
    singletonIndexedSeq(new PartialSortTask(input, workIdentity, this))
}
