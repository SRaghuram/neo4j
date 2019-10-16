/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, InstanceField, IntermediateRepresentation}
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler.ScopeContinuationState
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Operator task which takes an input morsel and produces one or many output rows
  * for each input row, and might require several operate calls to be fully executed.
  */
abstract class InputLoopTask extends ContinuableOperatorTaskWithMorsel {

  /**
    * Initialize the inner loop for the current input row.
    *
    * @return true iff the inner loop might result in output rows
    */
  protected def initializeInnerLoop(context: QueryContext,
                                    state: QueryState,
                                    resources: QueryResources,
                                    initExecutionContext: ExecutionContext): Boolean

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

  protected def enterOperate(context: QueryContext, state: QueryState, resources: QueryResources): Unit = {}
  protected def exitOperate(): Unit = {}

  private var innerLoop: Boolean = false

  override final def operate(outputRow: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources): Unit = {

    enterOperate(context, state, resources)

    while ((inputMorsel.isValidRow || innerLoop) && outputRow.isValidRow) {
      if (!innerLoop) {
        innerLoop = initializeInnerLoop(context, state, resources, outputRow)
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
    exitOperate()
  }

  override def canContinue: Boolean =
    inputMorsel.isValidRow || innerLoop

  override protected def closeCursors(resources: QueryResources): Unit = {
    // note: we always close, because `innerLoop` might not be reliable if
    // there has been an exception during `initializeInnerLoop`
    closeInnerLoop(resources)
  }
}

abstract class InputLoopTaskTemplate(override val inner: OperatorTaskTemplate,
                                     override val id: Id,
                                     innermost: DelegateOperatorTaskTemplate,
                                     protected val codeGen: OperatorExpressionCompiler,
                                     override val isHead: Boolean = true) extends ContinuableOperatorTaskWithMorselTemplate {
  import OperatorCodeGenHelperTemplates._

  protected val canContinue: InstanceField = field[Boolean](scopeId + "CanContinue")

  protected val innerLoop: InstanceField = field[Boolean](scopeId + "InnerLoop")

  private var continuationState: ScopeContinuationState = _

  override protected def scopeId: String = "leafOperator" + id.x

  // TODO: innerLoop and the first boolean field in continuationState (HasContinuationState) are redundant. Pick one when we have stable code/
  override final def genFields: Seq[Field] = Seq(canContinue, innerLoop) ++ continuationState.fields ++ genMoreFields

  def genMoreFields: Seq[Field]

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, loadField(canContinue))).orElse(Some(loadField(canContinue)))
  }

  override def genCloseCursors: IntermediateRepresentation = {
    block(
      // note: we always close, because `innerLoop` might not be reliable if
      // there has been an exception during `initializeInnerLoop`
      genCloseInnerLoop,
      inner.genCloseCursors)
  }

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  final override protected def genOperateHead: IntermediateRepresentation = {
    //// Based on this code from InputLoopTask
    //while ((inputMorsel.isValidRow || innerLoop) && outputRow.isValidRow) {
    //  if (!innerLoop) {
    //    innerLoop = initializeInnerLoop(context, state, resources) <<< genInitializeInnerLoop
    //  } else {
    //    // Continuation of ongoing inner loop. Restore the state of local variables
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
      labeledLoop(OUTER_LOOP_LABEL_NAME, and(or(INPUT_ROW_IS_VALID, loadField(innerLoop)), innermost.predicate))(
        {
          val body =
            block(
              // Initialize the inner loop
              genInitializeInnerLoopOrRestoreContinuationState,

              // Enter the inner loop if we have one for this input row
              ifElse(loadField(innerLoop))(
                block(
                  genScopeWithLocalDeclarations(scopeId + "innerLoop", genInnerLoop),
                  condition(not(loadField(canContinue)))(
                    block(
                      genCloseInnerLoop,
                      setField(innerLoop, constant(false)),
                      INPUT_ROW_MOVE_TO_NEXT,
                      setField(canContinue, INPUT_ROW_IS_VALID)
                    )
                  )
                )
              )( // Else move to the next input row
                block(
                  INPUT_ROW_MOVE_TO_NEXT
                )
              ),
              innermost.resetCachedPropertyVariables
            )
          // We generate the code for the whole loop body first, before we generate the cod for loading locals from input context slots.
          // This is because we need to know which ones are actually used.
          block(continuationState.assignments :+ body: _*)
        }
      )
    )
  }

  final override protected def genOperateMiddle: IntermediateRepresentation = {
    /**
      * This is called when the loop is used as a middle operator,
      * Here we should act as an inner loop and not advance the input
      * morsel
      * {{{
      *   this.canContinue = input.isValid
      *   while ( (this.canContinue || this.innerLoop) && hasDemand) {
      *     if (!this.innerLoop) {
      *       this.innerLoop = [genInitializeInnerLoop]
      *     }
      *     if (this.innerLoop) {
      *       [genInnerLoop]
      *       if (!this.canContinue) {
      *         [genCloseInnerLoop]
      *         this.innerLoop = false
      *       }
      *     }
      *   }
      * }}}
      */
    block(
      setField(canContinue, INPUT_ROW_IS_VALID),
      loop(and(or(loadField(canContinue), loadField(innerLoop)), innermost.predicate))(
        {
          val body =
            block(
              // Initialize the inner loop
              genInitializeInnerLoopOrRestoreContinuationState,

              // Enter the inner loop if we have one for this input row
              condition(loadField(innerLoop))(
                block(
                  genScopeWithLocalDeclarations(scopeId + "innerLoop", genInnerLoop),
                  condition(not(loadField(canContinue)))(
                    block(
                      genCloseInnerLoop,
                      setField(innerLoop, constant(false)),
                    )
                  )
                )
              ),
              innermost.resetCachedPropertyVariables,
              condition(and(loadField(canContinue), not(innermost.predicate)))(
                break(OUTER_LOOP_LABEL_NAME)
              )
            )
          // We generate the code for the whole loop body first, before we generate the cod for loading locals from input context slots.
          // This is because we need to know which ones are actually used.
          block(continuationState.assignments :+ body: _*)
        }
      )
    )
  }

  private def genInitializeInnerLoopOrRestoreContinuationState: IntermediateRepresentation = {
    ifElse(not(loadField(innerLoop)))(
      // Start a new inner loop
      block(
        // Record all the locals that are loaded from slots by the genInitializeInnerLoop code into continuationState
        // TODO: We can also incorporate the explicit fields that operators use (cursors etc.) so that we
        //       can use local variables and have a single continuation state per operator
        { codeGen.beginScope(scopeId + "init"); noop() },
        setField(innerLoop, genInitializeInnerLoop),
        { continuationState = codeGen.endInitializationScope(); noop() } // NOTE: We emit the saveStateIR in genOperateExit
      )
    )(
      // Continuation of ongoing inner loop. We need to restore the state of local variables
      continuationState.restoreStateIR
    )
  }

  override def genOperateExit: IntermediateRepresentation = {
    assert(continuationState != null)
    block(
      // If we still have an ongoing inner loop we need to save the local slot variables to fields
      condition(loadField(innerLoop))(
        continuationState.saveStateIR
      ),
      inner.genOperateExit
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

  /**
    * Closes the inner loop, allows the input loop to update variables before going
    * into next iteration of inner loop
    */
  protected def endInnerLoop: IntermediateRepresentation = innermost.resetCachedPropertyVariables
}
