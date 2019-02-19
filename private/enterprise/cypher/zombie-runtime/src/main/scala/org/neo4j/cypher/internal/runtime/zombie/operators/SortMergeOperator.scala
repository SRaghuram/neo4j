/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import java.util.{Comparator, PriorityQueue}

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateCreator, ArgumentStateMap, MorselAccumulator, Zombie}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.operators.MorselSorting
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

/**
  * Reducing operator which collects pre-sorted input morsels until it
  * has seen all, and then streams the rows from these morsels in
  * sorted order. Like all reducing operators, [[SortMergeOperator]]
  * collects and streams grouped by argument rows ids.
  */
class SortMergeOperator(val planId: Id,
                        val workIdentity: WorkIdentity,
                        orderBy: Seq[ColumnOrder],
                        argumentSlotOffset: Int,
                        countExpression: Option[Expression] = None) extends Operator {

  override def toString: String = "SortMerge"

  private val comparator: Comparator[MorselExecutionContext] =
    orderBy
    .map(MorselSorting.createMorselComparator)
    .reduce((a: Comparator[MorselExecutionContext], b: Comparator[MorselExecutionContext]) => a.thenComparing(b))

  override def createState(argumentStateCreator: ArgumentStateCreator): OperatorState = {
    new State(argumentStateCreator.createArgumentStateMap(planId, () => new UnsortedBuffer))
  }

  class UnsortedBuffer extends MorselAccumulator {
    private[SortMergeOperator] val data = new ArrayBuffer[MorselExecutionContext]
    override def update(morsel: MorselExecutionContext): Unit = {
      data += morsel
    }
  }

  class State(argumentStateMap: ArgumentStateMap[UnsortedBuffer]) extends OperatorState {
    override def init(queryContext: QueryContext,
                      state: QueryState,
                      input: MorselParallelizer,
                      resources: QueryResources
                     ): IndexedSeq[ContinuableInputOperatorTask] = {

      Array(new OTask(input.nextCopy, argumentStateMap))
    }
  }

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  class OTask(val inputMorsel: MorselExecutionContext,
              argumentStateMap: ArgumentStateMap[UnsortedBuffer]) extends ContinuableInputOperatorTask {

    override def toString: String = "SortMergeTask"

    var completedArguments: Iterator[UnsortedBuffer] = _
    var sortedInputPerArgument = new PriorityQueue[MorselExecutionContext](comparator)

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

//      Zombie.debug("SortMerge start")

      if (completedArguments == null) {
        argumentStateMap.updateAndConsume(inputMorsel)
        completedArguments = argumentStateMap.consumeCompleted()
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

        if (sortedInputPerArgument.isEmpty && completedArguments.hasNext) {
          val inputs = completedArguments.next()
          inputs.data.foreach { morsel =>
            if (morsel.hasData) sortedInputPerArgument.add(morsel)
          }
        }
      }

      outputRow.finishedWriting()

//      Zombie.debug("SortMerge end")
    }

    override def canContinue: Boolean = !sortedInputPerArgument.isEmpty || completedArguments.hasNext
  }
}
