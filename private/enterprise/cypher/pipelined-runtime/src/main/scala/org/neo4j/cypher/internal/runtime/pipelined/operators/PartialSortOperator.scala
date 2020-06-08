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
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
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
                         (val id: Id = Id.INVALID_ID) extends Operator {

  private type ResultsBuffer = util.ArrayList[MorselRow]
  private class ResultsBufferAndIndex(val buffer: ResultsBuffer, var currentIndex: Int)

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    new PartialSortState(stateFactory.memoryTracker)
  }

  override def toString: String = "PartialSortOperator"

  private class PartialSortState(val memoryTracker: QueryMemoryTracker, // TODO: Use operator MemoryTracker directly
                                 var lastSeen: MorselRow,
                                 var resultsBuffer: ResultsBuffer,
                                 var remainingResults: ResultsBufferAndIndex) extends OperatorState {
    def this(memoryTracker: QueryMemoryTracker) = this(memoryTracker, null, new ResultsBuffer, null)

    override def nextTasks(state: PipelinedQueryState,
                           operatorInput: OperatorInput,
                           parallelism: Int,
                           resources: QueryResources,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
      val input: MorselData = operatorInput.takeData()
      if (input != null) {
        singletonIndexedSeq(new PartialSortTask(input, this))
      } else {
        null
      }
    }
  }

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
      taskState.resultsBuffer.add(taskState.lastSeen)
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

    override def canContinue: Boolean = super.canContinue || taskState.remainingResults != null

    private def tryWriteOutstandingResults(outputCursor: MorselWriteCursor): Unit = {
      while (taskState.remainingResults != null && outputCursor.onValidRow()) {
        val resultsBufferAndIndex = taskState.remainingResults
        if (resultsBufferAndIndex.currentIndex >= resultsBufferAndIndex.buffer.size()) {
          taskState.remainingResults = null
        } else {
          val morselRow = resultsBufferAndIndex.buffer.get(resultsBufferAndIndex.currentIndex)
          outputCursor.copyFrom(morselRow)

          taskState.memoryTracker.deallocated(morselRow, id.x)
          resultsBufferAndIndex.buffer.set(resultsBufferAndIndex.currentIndex, null)
          resultsBufferAndIndex.currentIndex += 1
          outputCursor.next()
        }
      }
    }

    private def completeCurrentChunk(): Unit = {
      val buffer = taskState.resultsBuffer

      if(buffer.size() > 1) {
        buffer.sort(suffixComparator)
      }

      taskState.remainingResults = new ResultsBufferAndIndex(buffer, 0)
      taskState.lastSeen = null
      taskState.resultsBuffer = new ResultsBuffer
    }
  }
}
