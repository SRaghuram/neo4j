/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.Comparator
import java.util.PriorityQueue

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Reducing operator which collects pre-sorted input morsels until it
 * has seen all, and then streams the rows from these morsels in
 * sorted order. Like all reducing operators, [[SortMergeOperator]]
 * collects and streams grouped by argument rows ids.
 */
class SortMergeOperator(val argumentStateMapId: ArgumentStateMapId,
                        val workIdentity: WorkIdentity,
                        orderBy: Seq[ColumnOrder],
                        argumentSlotOffset: Int)
                       (val id: Id = Id.INVALID_ID)
  extends Operator
  with ReduceOperatorState[Morsel, ArgumentStateBuffer] {

  override def toString: String = "SortMerge"

  private val comparator: Comparator[MorselRow] = MorselSorting.createComparator(orderBy)

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: QueryState,
                           resources: QueryResources): ReduceOperatorState[Morsel, ArgumentStateBuffer] = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory, id))
    this
  }

  override def nextTasks(state: QueryState,
                         input: ArgumentStateBuffer,
                         resources: QueryResources
                        ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[Morsel, ArgumentStateBuffer]] = {
    Array(new OTask(input))
  }

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  class OTask(override val accumulator: ArgumentStateBuffer) extends ContinuableOperatorTaskWithAccumulator[Morsel, ArgumentStateBuffer] {

    override def workIdentity: WorkIdentity = SortMergeOperator.this.workIdentity

    override def toString: String = "SortMergeTask"

    var sortedInputPerArgument: PriorityQueue[MorselReadCursor] = _

    override def operate(outputMorsel: Morsel,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      if (sortedInputPerArgument == null) {
        sortedInputPerArgument = new PriorityQueue[MorselReadCursor](comparator)
        accumulator.foreach { morsel =>
          if (morsel.hasData) {
            sortedInputPerArgument.add(morsel.readCursor(onFirstRow = true))
          }
        }
      }

      val outputCursor = outputMorsel.writeCursor(onFirstRow = true)
      while (outputCursor.onValidRow && canContinue) {
        val nextRow: MorselReadCursor = sortedInputPerArgument.poll()
        outputCursor.copyFrom(nextRow)
        nextRow.next()
        outputCursor.next()
        // If there is more data in this Morsel, we'll re-insert it into the sortedInputs
        if (nextRow.onValidRow()) {
          sortedInputPerArgument.add(nextRow)
        }
      }

      outputCursor.truncate()
    }

    override def canContinue: Boolean = !sortedInputPerArgument.isEmpty
  }
}
