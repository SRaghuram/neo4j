/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel._

/**
  * Operator task which takes an input morsel and produces one or many output rows
  * for each input row, and might require several operate calls to be fully executed.
  */
abstract class InputLoopTask extends ContinuableOperatorTaskWithMorsel {

  /**
    * Initialize the inner loop for the current input row.
    *
    * @return true iff the inner loop might result it output rows
    */
  protected def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean

  /**
    * Execute the inner loop for the current input row, and write results to the output.
    */
  protected def innerLoop(outputRow: MorselExecutionContext,
                          context: QueryContext,
                          state: QueryState): Unit

  /**
    * Close any resources used by the inner loop.
    */
  protected def closeInnerLoop(resources: QueryResources): Unit

  private var innerLoop: Boolean = false

  override final def operate(outputRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {

    while ((inputMorsel.isValidRow || innerLoop) && outputRow.isValidRow) {
      if (!innerLoop) {
        innerLoop = initializeInnerLoop(context, state, resources)
      }
      // Do we have any output rows for this input row?
      if (innerLoop) {
        // Implementor is responsible for advancing both `outputRow` and `innerLoop`.
        // Typically the loop will look like this:
        //        while (outputRow.hasMoreRows && cursor.next()) {
        //          ... // Copy argumentSize #columns from inputRow to outputRow
        //          ... // Write additional columns to outputRow
        //          outputRow.moveToNextRow()
        //        }
        // The reason the loop itself is not already coded here is to avoid too many fine-grained virtual calls
        innerLoop(outputRow, context, state)

        // If we have not filled the output rows, move to the next input row
        if (outputRow.isValidRow) {
          // NOTE: There is a small chance that we run out of output rows and innerLoop iterations simultaneously where we would generate
          // an additional empty work unit that will just close the innerLoop. This could be avoided if we changed the innerLoop interface to something
          // slightly more complicated, but since innerLoop iterations and output morsel size will have to match exactly for this to happen it is
          // probably not a big problem in practice, and the additional checks required may not be worthwhile.
          closeInnerLoop(resources)
          innerLoop = false
          inputMorsel.moveToNextRow()
        }
      } else {
        // Nothing to do for this input row, move to the next
        inputMorsel.moveToNextRow()
      }
    }

    outputRow.finishedWriting()
  }

  override def canContinue: Boolean =
    inputMorsel.isValidRow || innerLoop
}

abstract class InputLoopTaskTemplate extends ContinuableOperatorTaskWithMorselTemplate {
  import OperatorCodeGenHelperTemplates._

  override def genFields: Seq[Field] = {
    Seq(DATA_READ, INPUT_MORSEL, INNER_LOOP)
  }


  override def genCanContinue: IntermediateRepresentation = {
    /** {{{inputMorsel.isValidRow || innerLoop}}}*/
    or(INPUT_ROW_IS_VALID, loadField(INNER_LOOP))
  }

  override def genOperate: IntermediateRepresentation = {
    //// Based on this code from InputLoopTask
    //while ((inputMorsel.isValidRow || innerLoop) && outputRow.isValidRow) {
    //  if (!innerLoop) {
    //    innerLoop = initializeInnerLoop(context, state, resources) <<< genInitializeInnerLoop
    //  }
    //  // Do we have any output rows for this input row?
    //  if (innerLoop) {
    //    // Implementor is responsible for advancing both `outputRow` and `innerLoop`.
    //    // Typically the loop will look like this:
    //    //        while (outputRow.hasMoreRows && cursor.next()) {
    //    //          ... // Copy argumentSize #columns from inputRow to outputRow
    //    //          ... // Write additional columns to outputRow
    //    //          outputRow.moveToNextRow()
    //    //        }
    //    // The reason the loop itself is not already coded here is to avoid too many fine-grained virtual calls
    //    innerLoop(outputRow, context, state) <<< genInnerLoop
    //
    //    // If we have not filled the output rows, move to the next input row
    //    if (outputRow.isValidRow) {
    //      // NOTE: There is a small chance that we run out of output rows and innerLoop iterations simultaneously where we would generate
    //      // an additional empty work unit that will just close the innerLoop. This could be avoided if we changed the innerLoop interface to something
    //      // slightly more complicated, but since innerLoop iterations and output morsel size will have to match exactly for this to happen it is
    //      // probably not a big problem in practice, and the additional checks required may not be worthwhile.
    //      closeInnerLoop(resources) <<< genCloseInnerLoop
    //      innerLoop = false
    //      inputMorsel.moveToNextRow()
    //    }
    //  }
    //  else {
    //    // Nothing to do for this input row, move to the next
    //    inputMorsel.moveToNextRow()
    //  }
    //}
    //
    //outputRow.finishedWriting()
    block(
      loop(
        and(
          or(INPUT_ROW_IS_VALID, loadField(INNER_LOOP)),
          OUTPUT_ROW_IS_VALID
        )
      )(
        block(
          condition(not(loadField(INNER_LOOP)))(setField(INNER_LOOP, genInitializeInnerLoop)),
          ifElse(loadField(INNER_LOOP))(
            block(
              genInnerLoop,
              condition(OUTPUT_ROW_IS_VALID)(
                block(
                  genCloseInnerLoop,
                  setField(INNER_LOOP, constant(false)),
                  INPUT_ROW_MOVE_TO_NEXT
                )
              )
            )
          )( //else
            INPUT_ROW_MOVE_TO_NEXT
          )
        )
      ),
      OUTPUT_ROW_FINISHED_WRITING
    )
  }

  /**
    * Responsible for generating method:
    * {{{
    *   def initializeInnerLoop(context: QueryContext,
    *                           state: QueryState,
    *                           resources: QueryResources): Boolean
    * }}}
    */
  protected def genInitializeInnerLoop: IntermediateRepresentation

  /**
    * Execute the inner loop for the current input row, and write results to the output.
    *
    * Responsible for generating:
    * {{{
    *   def innerLoop(outputRow: MorselExecutionContext,
    *                 context: QueryContext,
    *                 state: QueryState): Unit
    * }}}
    */
  protected def genInnerLoop: IntermediateRepresentation

  /**
    * Close any resources used by the inner loop.
    *
    * Responsible for generating:
    * {{{
    *    def closeInnerLoop(resources: QueryResources): Unit
    * }}}
    */
  protected def genCloseInnerLoop: IntermediateRepresentation
}
