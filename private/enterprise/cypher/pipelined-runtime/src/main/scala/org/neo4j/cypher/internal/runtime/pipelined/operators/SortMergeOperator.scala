/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.{Comparator, PriorityQueue}

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.{PipelinedExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder

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
  extends Operator
     with ReduceOperatorState[PipelinedExecutionContext, ArgumentStateBuffer] {

  override def toString: String = "SortMerge"

  private val comparator: Comparator[PipelinedExecutionContext] = MorselSorting.createComparator(orderBy)

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): ReduceOperatorState[PipelinedExecutionContext, ArgumentStateBuffer] = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory))
    this
  }

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         input: ArgumentStateBuffer,
                         resources: QueryResources
                        ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[PipelinedExecutionContext, ArgumentStateBuffer]] = {
    Array(new OTask(input))
  }

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  class OTask(override val accumulator: ArgumentStateBuffer) extends ContinuableOperatorTaskWithAccumulator[PipelinedExecutionContext, ArgumentStateBuffer] {

    override def workIdentity: WorkIdentity = SortMergeOperator.this.workIdentity

    override def toString: String = "SortMergeTask"

    var sortedInputPerArgument: PriorityQueue[PipelinedExecutionContext] = _

    override def operate(outputRow: PipelinedExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      if (sortedInputPerArgument == null) {
        sortedInputPerArgument = new PriorityQueue[PipelinedExecutionContext](comparator)
        accumulator.foreach { morsel =>
          if (morsel.hasData) {
            sortedInputPerArgument.add(morsel)
          }
        }
      }

      while (outputRow.isValidRow && canContinue) {
        val nextRow: PipelinedExecutionContext = sortedInputPerArgument.poll()
        outputRow.copyFrom(nextRow)
        nextRow.moveToNextRow()
        outputRow.moveToNextRow()
        // If there is more data in this Morsel, we'll re-insert it into the sortedInputs
        if (nextRow.isValidRow) {
          sortedInputPerArgument.add(nextRow)
        }
      }

      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = !sortedInputPerArgument.isEmpty
  }
}
