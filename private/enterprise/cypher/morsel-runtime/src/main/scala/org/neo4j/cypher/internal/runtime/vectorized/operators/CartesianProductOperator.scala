/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.vectorized._

class CartesianProductOperator(val workIdentity: WorkIdentity, override val argumentSize: SlotConfiguration.Size)
  extends StreamingMergeOperator[RhsPipelineArgument] {

  override def initFromLhs(queryContext: QueryContext,
                           state: QueryState,
                           lhsInputMorsel: MorselExecutionContext,
                           cursors: ExpressionCursors): (Option[ContinuableOperatorTask], Option[RhsPipelineArgument]) = {
    if (lhsInputMorsel.hasMoreRows) {
      val pipelineArgument = RhsPipelineArgument(lhsInputMorsel)
      (None, Some(pipelineArgument)) // Returning Some argument here will make the pipeline schedule a task for the right-hand side
    }
    else {
      // Do not schedule any new tasks if we received an empty input morsel
      (None, None)
    }
  }

  override def initFromRhs(queryContext: QueryContext,
                           state: QueryState,
                           rhsInputMorsel: MorselExecutionContext,
                           cursors: ExpressionCursors,
                           pipelineArgument: RhsPipelineArgument): Option[ContinuableOperatorTask] = {
    val lhsInputMorsel = pipelineArgument.lhsMorsel

    // We need to copy the ExecutionContext around the Morsel so we can have our own iteration state
    val newLhsInputMorsel = lhsInputMorsel.shallowCopy()
    newLhsInputMorsel.resetToFirstRow()

    Some(new MergeOTask(newLhsInputMorsel, rhsInputMorsel))
  }

  class MergeOTask(lhsInputRow: MorselExecutionContext, rhsInputRow: MorselExecutionContext) extends ContinuableOperatorTask {

    override def operate(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit = {
      // Checking rhsInputRow.hasMoreRows in the outer loop may not appear to be strictly necessary, but it will prevent an infinite loop
      // in case we for some reason would receive an empty morsel in lhsInputRow
      while ((lhsInputRow.hasMoreRows || rhsInputRow.hasMoreRows) && outputRow.hasMoreRows) {
        while (rhsInputRow.hasMoreRows && outputRow.hasMoreRows) {
          lhsInputRow.copyTo(outputRow)
          rhsInputRow.copyTo(outputRow,
            fromLongOffset = argumentSize.nLongs, fromRefOffset = argumentSize.nReferences, // Skip over arguments since they should be identical to lhsInputRow
            toLongOffset = lhsInputRow.getLongsPerRow, toRefOffset = lhsInputRow.getRefsPerRow)

          outputRow.moveToNextRow()
          rhsInputRow.moveToNextRow()
        }
        // If we have exhausted the rhs input rows, move to the next lhs input row
        if (!rhsInputRow.hasMoreRows) {
          lhsInputRow.moveToNextRow()
          // If we are not finished with the lhs input rows, reset the rhs input row to combine its morsel with the new lhs input row
          if (lhsInputRow.hasMoreRows) {
            rhsInputRow.resetToFirstRow()
          }
        }
      }

      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = lhsInputRow.hasMoreRows || rhsInputRow.hasMoreRows
  }

}

case class RhsPipelineArgument(lhsMorsel: MorselExecutionContext) extends PipelineArgument
