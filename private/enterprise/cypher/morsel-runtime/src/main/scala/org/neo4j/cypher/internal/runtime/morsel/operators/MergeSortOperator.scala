/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util.{Comparator, PriorityQueue}

import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.storable.NumberValue

/**
  * This operator takes pre-sorted inputs, and merges them together, producing a stream of Morsels with the sorted data
  * If countExpression != None, this expression evaluates to a limit for TopN
  */
class MergeSortOperator(val workIdentity: WorkIdentity,
                        orderBy: Seq[ColumnOrder],
                        countExpression: Option[Expression] = None) extends EagerReduceOperator {

  private val comparator: Comparator[MorselExecutionContext] = orderBy
    .map(MorselSorting.createMorselComparator)
    .reduce((a: Comparator[MorselExecutionContext], b: Comparator[MorselExecutionContext]) => a.thenComparing(b))

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputs: Seq[MorselExecutionContext],
                    resources: QueryResources): ContinuableOperatorTask = {

    val sortedInputs = new PriorityQueue[MorselExecutionContext](inputs.length, comparator)
    inputs.foreach { row =>
      if (row.hasData) sortedInputs.add(row)
    }

    val limit = countExpression.map { count =>
      val firstRow = sortedInputs.peek()
      val queryState = new OldQueryState(queryContext,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession])
      count(firstRow, queryState).asInstanceOf[NumberValue].longValue()
    }

    new OTask(sortedInputs, limit.getOrElse(Long.MaxValue))
  }

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  class OTask(val sortedInputs: PriorityQueue[MorselExecutionContext], val limit: Long) extends ContinuableOperatorTask {

    var totalPos = 0

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      while(!sortedInputs.isEmpty && outputRow.isValidRow && totalPos < limit) {
        val nextRow: MorselExecutionContext = sortedInputs.poll()
        outputRow.copyFrom(nextRow)
        totalPos += 1
        nextRow.moveToNextRow()
        outputRow.moveToNextRow()
        // If there is more data in this Morsel, we'll re-insert it into the sortedInputs
        if (nextRow.isValidRow) {
          sortedInputs.add(nextRow)
        }
      }

      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = !sortedInputs.isEmpty && totalPos < limit
  }
}

