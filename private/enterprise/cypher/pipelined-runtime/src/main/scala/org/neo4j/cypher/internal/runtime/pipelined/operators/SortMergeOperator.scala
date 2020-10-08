/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.Comparator

import org.neo4j.cypher.internal.collection.DefaultComparatorSortTable
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

/**
 * Reducing operator which collects pre-sorted input morsels until it
 * has seen all, and then streams the rows from these morsels in
 * sorted order. Like all reducing operators, [[SortMergeOperator]]
 * collects and streams grouped by argument rows ids.
 */
class SortMergeOperator(val argumentStateMapId: ArgumentStateMapId,
                        val workIdentity: WorkIdentity,
                        comparator: Comparator[ReadableRow])
                       (val id: Id = Id.INVALID_ID)
  extends Operator
  with AccumulatorsInputOperatorState[Morsel, ArgumentStateBuffer] {

  override def toString: String = "SortMerge"

  override def accumulatorsPerTask(morselSize: Int): Int = 1

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): AccumulatorsInputOperatorState[Morsel, ArgumentStateBuffer] = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory, id), memoryTracker)
    new MemoryTrackingAccumulatorsInputOperatorState(this, id.x, stateFactory)
  }

  override def nextTasks(input: IndexedSeq[ArgumentStateBuffer],
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[Morsel, ArgumentStateBuffer]] = {
    singletonIndexedSeq(new OTask(input, resources.memoryTracker))
  }

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  class OTask(override val accumulators: IndexedSeq[ArgumentStateBuffer], memoryTracker: MemoryTracker)
    extends ContinuableOperatorTaskWithAccumulators[Morsel, ArgumentStateBuffer] {

    override def workIdentity: WorkIdentity = SortMergeOperator.this.workIdentity

    override def toString: String = "SortMergeTask"

    AssertMacros.checkOnlyWhenAssertionsAreEnabled(accumulators.size == 1)
    private val accumulator = accumulators.head
    private var sortedInputPerArgument: DefaultComparatorSortTable[MorselReadCursor] = _

    override def operate(outputMorsel: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = {
      if (sortedInputPerArgument == null) {
        sortedInputPerArgument = new DefaultComparatorSortTable(comparator, 16, memoryTracker)
        // Since we retain the memory of every morsel until the complete sort is finished,
        // and they are already tracked by the inner buffer inside the accumulator, we do not take() the morsels here (which would release the heap usage).
        // Instead we close() the accumulator when we are done with all of them (in closeInput below).
        accumulator.foreach { morsel =>
          if (morsel.hasData) {
            sortedInputPerArgument.add(morsel.readCursor(onFirstRow = true))
          }
        }
      }

      val outputCursor = outputMorsel.writeCursor()
      while (outputCursor.next() && canContinue) {
        val nextRow: MorselReadCursor = sortedInputPerArgument.peek()
        outputCursor.copyFrom(nextRow)
        // If there is more data in this Morsel, we will siftDown based on the new row
        if (nextRow.next()) {
          sortedInputPerArgument.siftDown(nextRow)
        } else {
          sortedInputPerArgument.poll() // Otherwise we remove it
        }
      }

      outputCursor.truncate()
    }

    override def canContinue: Boolean = !sortedInputPerArgument.isEmpty

    override def closeInput(operatorCloser: OperatorCloser): Unit = {
      super.closeInput(operatorCloser)
      accumulator.close()
      if (sortedInputPerArgument != null) {
        sortedInputPerArgument.close()
        sortedInputPerArgument = null;
      }
    }
  }
}
