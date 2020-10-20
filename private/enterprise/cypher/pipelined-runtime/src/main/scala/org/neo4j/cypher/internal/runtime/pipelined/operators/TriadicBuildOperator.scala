/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

class TriadicBuildOperator(override val workIdentity: WorkIdentity,
                           bufferAsmId: ArgumentStateMapId,
                           triadicStateAsmId: ArgumentStateMapId,
                           sourceOffset: Int,
                           seenOffset: Int,
                           argumentOffset: Int,
                           id: Id) extends Operator {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(bufferAsmId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)

    val memoryTracker = stateFactory.newMemoryTracker(id.x)

    // `triadicStateAsmId` is an ID of a map that should be shared between triadic build and filter operators.
    // We want to make sure that we don't accidentally create a second map, hence `createOrGetArgumentStateMap`.
    val triadicStateAsm = argumentStateCreator.createOrGetArgumentStateMap(triadicStateAsmId, new operators.TriadicState.Factory(memoryTracker), ordered = true)

    new TriadicBuildTaskState(memoryTracker, workIdentity, triadicStateAsm, sourceOffset, seenOffset, argumentOffset)
  }
}

class TriadicBuildTaskState(memoryTracker: MemoryTracker,
                            workIdentity: WorkIdentity,
                            triadicStateAsm: ArgumentStateMap[TriadicState],
                            sourceOffset: Int,
                            seenOffset: Int,
                            argumentOffset: Int) extends DataInputOperatorState[MorselData] {

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
    if (lastSeen != null && lastSeen.getLongAt(sourceOffset) != inputCursor.getLongAt(sourceOffset)) {
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
    if (lastSeen != null) {
      completeCurrentChunk()
    }
  }

  // After a chunk is completed, writes buffered rows to output. Only
  // writes as many rows as output morsel can take, so might require
  // multiple calls before we can start buffering rows from next chunk.
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
    val argumentRowId = ArgumentSlots.getArgumentAt(lastSeen, argumentOffset)
    val sourceKey = lastSeen.getLongAt(sourceOffset)
    triadicStateAsm.update(argumentRowId, _.addSeenRows(sourceKey, buffer, seenOffset))

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

  override def nextTasks(state: PipelinedQueryState, input: MorselData, argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] =
    singletonIndexedSeq(new TriadicBuildTask(input, workIdentity, this))
}

class TriadicBuildTask(morselData: MorselData,
                       override val workIdentity: WorkIdentity,
                       taskState: TriadicBuildTaskState) extends InputLoopWithMorselDataTask(morselData) {

  override def toString: String = "TriadicBuildTask"

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

  override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit = {
    taskState.tryWriteOutstandingResults(outputCursor)
  }

  override def canContinue: Boolean = super.canContinue || taskState.hasOutputToWrite
}
