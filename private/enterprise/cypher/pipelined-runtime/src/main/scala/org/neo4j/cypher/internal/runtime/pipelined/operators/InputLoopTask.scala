/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NEXT
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Operator task which takes an input morsel and produces zero or more output rows
 * for each input row, and might require several operate calls to be fully executed.
 */
abstract class InputLoopTask(final val inputMorsel: Morsel) extends ContinuableOperatorTaskWithMorsel {

  final val inputCursor: MorselReadCursor = inputMorsel.readCursor(onFirstRow = true)

  /**
   * Initialize the inner loop for the current input row.
   *
   * @return true iff the inner loop might result in output rows
   */
  protected def initializeInnerLoop(state: PipelinedQueryState,
                                    resources: QueryResources,
                                    initExecutionContext: ReadWriteRow): Boolean

  /**
   * Execute the inner loop for the current input row, and write results to the output.
   */
  protected def innerLoop(outputRow: MorselFullCursor,
                          state: PipelinedQueryState): Unit

  /**
   * Close any resources used by the inner loop.
   */
  protected def closeInnerLoop(resources: QueryResources): Unit

  protected def enterOperate(state: PipelinedQueryState, resources: QueryResources): Unit = {}
  protected def exitOperate(): Unit = {}

  private var innerLoop: Boolean = false

  override final def operate(outputMorsel: Morsel,
                             state: PipelinedQueryState,
                             resources: QueryResources): Unit = {
    //In the situation we get here with the inputCursor standing on a cancelled
    //row we should advance to the next non-cancelled row
    advanceOnCancelledRows(resources)
    enterOperate(state, resources)
    val outputCursor = outputMorsel.fullCursor(onFirstRow = true)

    while ((inputCursor.onValidRow || innerLoop) && outputCursor.onValidRow) {
      if (!innerLoop) {
        innerLoop = initializeInnerLoop(state, resources, outputCursor)
      }
      // Do we have any output rows for this input row?
      if (innerLoop) {
        // Implementor is responsible for advancing both `outputRow` and `innerLoop`.
        // Typically the loop will look like this:
        //        while (outputRow.hasNext && cursor.next()) {
        //          ... // Copy argumentSize #columns from inputRow to outputRow
        //          ... // Write additional columns to outputRow
        //          outputRow.next()
        //        }
        // The reason the loop itself is not already coded here is to avoid too many fine-grained virtual calls
        innerLoop(outputCursor, state)

        // If we have not filled the output rows, move to the next input row
        if (outputCursor.onValidRow()) {
          // NOTE: There is a small chance that we run out of output rows and innerLoop iterations simultaneously where we would generate
          // an additional empty work unit that will just close the innerLoop. This could be avoided if we changed the innerLoop interface to something
          // slightly more complicated, but since innerLoop iterations and output morsel size will have to match exactly for this to happen it is
          // probably not a big problem in practice, and the additional checks required may not be worthwhile.
          closeInnerLoop(resources)
          innerLoop = false
          inputCursor.next()
        }
      } else {
        // Nothing to do for this input row, move to the next
        inputCursor.next()
      }
    }

    outputCursor.truncate()
    exitOperate()
  }

  private def advanceOnCancelledRows(resources: QueryResources): Unit = {
    if (!inputCursor.onValidRow()) {
      //If we were in the process of executing an inner loop we must now
      //close it and pick up a new inner loop
      if (innerLoop) {
        closeInnerLoop(resources)
        innerLoop = false
      }
      inputCursor.next()
    }
  }

  override def canContinue: Boolean =
    inputCursor.onValidRow || innerLoop

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

  protected val canContinue: InstanceField = field[Boolean](scopeId + "CanContinue")

  protected val innerLoop: InstanceField = field[Boolean](scopeId + "InnerLoop")

  override protected def scopeId: String = "leafOperator" + id.x

  override final def genFields: Seq[Field] = Seq(canContinue, innerLoop) ++ genMoreFields

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

  final override protected def genOperateHead(profile: Boolean): IntermediateRepresentation = {
    //// Based on this code from InputLoopTask
    //val outputCursor = outputRow.fullCursor(onFirstRow = true)
    //if (!this.inputCursor.onValidRow) {
    //  if (innerLoop) {
    //     [close]
    //      innerLoop = false
    //  }
    //  this.inputCursor.next()
    //while ((inputCursor.onValidRow || innerLoop) && outputCursor.onValidRow) {
    //  if (!innerLoop) {
    //    innerLoop = initializeInnerLoop(context, state, resources, outputCursor) <<< genInitializeInnerLoop
    //  } else {
    //    // Continuation of ongoing inner loop. Restore the state of local variables
    //  }
    //  // Do we have any output rows for this input row?
    //  if (innerLoop) {
    //    innerLoop(outputCursor, context, state) <<< genInnerLoop
    //
    //    // If we have not filled the output rows, move to the next input row
    //    if (outputCursor.onValidRow()) {
    //      closeInnerLoop(resources) <<< genCloseInnerLoop
    //      innerLoop = false
    //      inputCursor.next()
    //    }
    //  } else {
    //    // Nothing to do for this input row, move to the next
    //    inputCursor.next()
    //  }
    //}
    //
    //outputCursor.close()
    block(
      genAdvanceOnCancelledRow,
      loop(and(or(INPUT_ROW_IS_VALID, loadField(innerLoop)), innermost.predicate))(
        block(
          innermost.resetBelowLimitAndAdvanceToNextArgument,
          // Initialize the inner loop
          doInitializeInnerLoopOrRestoreContinuationState(profile),

          // Enter the inner loop if we have one for this input row
          ifElse(loadField(innerLoop))(
            block(
              genScopeWithLocalDeclarations(scopeId + "innerLoop", genInnerLoop(profile)),
              condition(not(loadField(canContinue)))(
                block(
                  genCloseInnerLoop,
                  setField(innerLoop, constant(false)),
                  doIfInnerCantContinue(setField(canContinue, invoke(INPUT_CURSOR, NEXT)))
                )
              )
            )
          )(
            // Else if no inner operator can proceed we move to the next input row
            doIfInnerCantContinue(invokeSideEffect(INPUT_CURSOR, NEXT))
          ),
          innermost.resetCachedPropertyVariables
        )
      )
    )
  }

  final override protected def genOperateMiddle(profile: Boolean): IntermediateRepresentation = {
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
        block(
          // Initialize the inner loop
          doInitializeInnerLoopOrRestoreContinuationState(profile),

          // Enter the inner loop if we have one for this input row
          condition(loadField(innerLoop))(
            block(
              genScopeWithLocalDeclarations(scopeId + "innerLoop", genInnerLoop(profile)),
              condition(not(loadField(canContinue)))(
                block(
                  genCloseInnerLoop,
                  setField(innerLoop, constant(false)),
                )
              )
            )
          ),
          innermost.resetCachedPropertyVariables,
        )
      )
    )
  }

  override def genClearStateOnCancelledRow: IntermediateRepresentation =
    block(
      condition(loadField(innerLoop)) {
        genCloseInnerLoop
      },
      setField(innerLoop, constant(false)),
      setField(canContinue, constant(false)),
    )

  //noinspection MutatorLikeMethodIsParameterless
  private def doInitializeInnerLoopOrRestoreContinuationState(profile: Boolean): IntermediateRepresentation = {
    // TODO: In this initialization scope we can record all the operator state variables (cursors etc.) that are now generated as explicit fields
    //       and instead use local variables together with a ScopeContinuationState containing the fields that needs to be
    //       saved in genOperateExit() and restored here in an `else` branch when the operator is called with a continuation.
    codeGen.beginScope(scopeId + "init")
    val body =
      condition(not(loadField(innerLoop)))(
        // Start a new inner loop
        block(
          setField(innerLoop, genInitializeInnerLoop(profile)),
        )
      )
    // Declarations need to be handled in the parent scope, since the same slot variables may be used in other sibling scopes (i.e. innerLoop)
    // (This works by setting mergeIntoParentScope to true, which will then result in the locals encountered in this scope appearing in the parent scope
    //  with the flag `initialized` set to true, which means they will be declared but not initialized from the input context)
    val localsState = codeGen.endScope(mergeIntoParentScope = true)
    // But we do generate the initialization assignments from input context here _before_ the conditional body, for every code path
    block(localsState.assignments :+ body: _*)
  }

  /**
   * Responsible for generating method:
   * {{{
   *   def initializeInnerLoop(context: QueryContext,
   *                           state: QueryState,
   *                           resources: QueryResources): Boolean
   * }}}
   */
  protected def genInitializeInnerLoop(profile: Boolean): IntermediateRepresentation

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
  protected def genInnerLoop(profile: Boolean): IntermediateRepresentation

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
