/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import java.util.{Comparator, PriorityQueue}

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.operators.MorselSorting
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.runtime.zombie.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.zombie.operators.SortMergeOperator.UnsortedBuffer
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.MorselAccumulator

import scala.collection.mutable.ArrayBuffer

/**
  * Reducing operator which collects pre-sorted input morsels until it
  * has seen all, and then streams the rows from these morsels in
  * sorted order. Like all reducing operators, [[SortMergeOperator]]
  * collects and streams grouped by argument rows ids.
  */
class SortMergeOperator(val argumentStateMapId: ArgumentStateMapId,
                        val workIdentity: WorkIdentity,
                        orderBy: Seq[ColumnOrder],
                        argumentSlotOffset: Int,
                        countExpression: Option[Expression] = None) extends Operator with ReduceOperatorState[UnsortedBuffer] {

  override def toString: String = "SortMerge"

  private val comparator: Comparator[MorselExecutionContext] =
    orderBy
    .map(MorselSorting.createMorselComparator)
    .reduce((a: Comparator[MorselExecutionContext], b: Comparator[MorselExecutionContext]) => a.thenComparing(b))

  override def createState(argumentStateCreator: ArgumentStateMapCreator): ReduceOperatorState[UnsortedBuffer] = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, argumentRowId => new UnsortedBuffer(argumentRowId))
    this
  }

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         input: UnsortedBuffer,
                         resources: QueryResources
                        ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[UnsortedBuffer]] = {
    Array(new OTask(input))
  }

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  class OTask(override val accumulator: UnsortedBuffer) extends ContinuableOperatorTaskWithAccumulator[UnsortedBuffer] {

    override def toString: String = "SortMergeTask"

    var sortedInputPerArgument: PriorityQueue[MorselExecutionContext] = _

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      if (sortedInputPerArgument == null) {
        sortedInputPerArgument = new PriorityQueue[MorselExecutionContext](comparator)
        accumulator.data.foreach { morsel =>
          if (morsel.hasData) {
            sortedInputPerArgument.add(morsel)
          }
        }
      }

      while (outputRow.isValidRow && canContinue) {
        while (outputRow.isValidRow && !sortedInputPerArgument.isEmpty) {
          val nextRow: MorselExecutionContext = sortedInputPerArgument.poll()
          outputRow.copyFrom(nextRow)
          nextRow.moveToNextRow()
          outputRow.moveToNextRow()
          // If there is more data in this Morsel, we'll re-insert it into the sortedInputs
          if (nextRow.isValidRow) {
            sortedInputPerArgument.add(nextRow)
          }
        }
      }

      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = !sortedInputPerArgument.isEmpty
  }
}

object SortMergeOperator {

  class UnsortedBuffer(override val argumentRowId: Long) extends MorselAccumulator {
    private[SortMergeOperator] val data = new ArrayBuffer[MorselExecutionContext]

    override def update(morsel: MorselExecutionContext): Unit = {
      data += morsel
    }

    override def toString: String = s"UnsortedBuffer($argumentRowId, count=${data.size})"
  }
}
