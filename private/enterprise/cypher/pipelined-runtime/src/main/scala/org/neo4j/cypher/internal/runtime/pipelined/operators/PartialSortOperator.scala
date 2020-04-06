/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Comparator

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

class PartialSortOperator(val argumentStateMapId: ArgumentStateMapId,
                          val workIdentity: WorkIdentity,
                          prefixComparator: Comparator[MorselRow],
                          suffixComparator: Comparator[MorselRow])
                         (val id: Id = Id.INVALID_ID) extends Operator with OperatorState {

  private type Buffer = util.ArrayList[MorselRow]
  private type ChunkList = util.LinkedList[Buffer]

  private class PartialSortState(val memoryTracker: QueryMemoryTracker,
                                 var lastSeen: MorselRow,
                                 var buffer: Buffer,
                                 var chunks: ChunkList) {
    def this(memoryTracker: QueryMemoryTracker) = this(memoryTracker, null, new Buffer, new ChunkList)
  }

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    taskState = new PartialSortState(stateFactory.memoryTracker)
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    this
  }

  override def toString: String = "PartialSortOperator"

  override def nextTasks(state: PipelinedQueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    val input: MorselData = operatorInput.takeData()
    if (input != null) {
      IndexedSeq(new PartialSortTask(input, taskState))
    } else {
      null
    }
  }

  private var taskState: PartialSortState = _

  class PartialSortTask(morselData: MorselData,
                        taskState: PartialSortState) extends InputLoopWithMorselDataTask(morselData) {

    override def workIdentity: WorkIdentity = PartialSortOperator.this.workIdentity

    override def toString: String = "PartialSortTask"

    override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = ()

    override def processRow(outputCursor: MorselWriteCursor,
                            inputCursor: MorselReadCursor): Unit = {
      // if new chunk
      if (taskState.lastSeen != null && prefixComparator.compare(taskState.lastSeen, inputCursor) != 0) {
        completeCurrentChunk()
        tryWriteOutstandingResults(outputCursor)
      }
      taskState.lastSeen = inputCursor.snapshot()
      taskState.buffer.add(taskState.lastSeen)
      taskState.memoryTracker.allocated(taskState.lastSeen, id.x)
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit =
      morselData.argumentStream match {
        case EndOfNonEmptyStream =>
          if (taskState.lastSeen != null)
            completeCurrentChunk()
        case _ =>
        // Do nothing
      }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit =
      tryWriteOutstandingResults(outputCursor)

    override def canContinue: Boolean = super.canContinue || !taskState.chunks.isEmpty

    private def tryWriteOutstandingResults(outputCursor: MorselWriteCursor): Unit = {
      while (!taskState.chunks.isEmpty && outputCursor.onValidRow()) {
        val chunk = taskState.chunks.getFirst
        val it = chunk.iterator()
        if (!it.hasNext) {
          taskState.chunks.removeFirst()
        } else {
          val morselRow = it.next()
          outputCursor.copyFrom(morselRow)

          taskState.memoryTracker.deallocated(morselRow, id.x)
          it.remove()
          outputCursor.next()
        }
      }
    }

    private def completeCurrentChunk(): Unit = {
      val buffer = taskState.buffer

      if(buffer.size() > 1) {
        buffer.sort(suffixComparator)
      }

      taskState.chunks.add(buffer)
      taskState.lastSeen = null
      taskState.buffer = new Buffer
    }
  }
}
