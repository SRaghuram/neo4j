/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, WorkUnitEvent}
import org.neo4j.cypher.internal.runtime.zombie.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.{DbAccess, ExpressionCursors, QueryContext}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.AnyValue

/**
  * Input to use for starting an operator task.
  */
trait OperatorInput {

  /**
    * Take the next input morsel
    *
    * @return the input morsel, or `null` if no input is available
    */
  def takeMorsel(): MorselParallelizer

  /**
    * Take the next input accumulator
    *
    * @return the input accumulator, or `null` if no input is available
    */
  def takeAccumulator[ACC <: MorselAccumulator](): ACC

  /**
    * Take the next input accumulator from the LHS and morsel from the RHS.
    *
    * @return the input accumulator and the morsel, or `null` if no input is available
    */
  def takeAccumulatorAndMorsel[ACC <: MorselAccumulator](): AccumulatorAndMorsel[ACC]
}

/**
  * Closer for ContinuableOperatorTasks.
  */
trait OperatorCloser {

  /**
    * Close input morsel.
    */
  def closeMorsel(morsel: MorselExecutionContext): Unit

  /**
    * Close input accumulators.
    */
  def closeAccumulator[ACC <: MorselAccumulator](accumulator: ACC): Unit

  def closeMorselAndAccumulatorTask[ACC <: MorselAccumulator](morsel: MorselExecutionContext, accumulator: ACC): Unit

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    *
    * @return `true` if the morsel is completely empty after cancellations
    */
  def filterCancelledArguments(morsel: MorselExecutionContext): Boolean

  /**
    * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
    *
    * @return `true` if the accumulator was removed
    */
  def filterCancelledArguments[ACC <: MorselAccumulator](accumulator: ACC): Boolean

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
    *
    * @param morsel the input morsel
    * @param accumulator the accumulator
    * @return `true` iff both the morsel and the accumulator are cancelled
    */
  def filterCancelledArguments[ACC <: MorselAccumulator](morsel: MorselExecutionContext, accumulator: ACC): Boolean
}

/**
  * A executable morsel operator.
  */
trait Operator extends HasWorkIdentity {

  /**
    * Create a new execution state for this operator.
    *
    * @param argumentStateCreator creator used to construct a argumentStateMap for this operator state
    * @return the new execution state for this operator.
    */
  def createState(argumentStateCreator: ArgumentStateMapCreator): OperatorState
}

/**
  * The execution state of an operator. One instance of this is created for every query execution.
  */
trait OperatorState {

  /**
    * Initialize new tasks for this operator. This code path let's operators create
    * multiple output rows for each row in `inputMorsel`.
    */
  def nextTasks(context: QueryContext,
                state: QueryState,
                operatorInput: OperatorInput,
                resources: QueryResources): IndexedSeq[ContinuableOperatorTask]
}

/**
  * The execution state of a reduce operator. One instance of this is created for every query execution.
  */
trait ReduceOperatorState[ACC <: MorselAccumulator] extends OperatorState {

  final override def nextTasks(context: QueryContext,
                               state: QueryState,
                               operatorInput: OperatorInput,
                               resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulator[ACC]] = {
    val input = operatorInput.takeAccumulator[ACC]()
    if (input != null) {
      nextTasks(context, state, input, resources)
    } else {
      null
    }
  }

  /**
    * Initialize new tasks for this operator.
    */
  def nextTasks(context: QueryContext,
                state: QueryState,
                input: ACC,
                resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulator[ACC]]
}

/**
  * A streaming operator is initialized with an input, to produce 0-n [[ContinuableOperatorTask]].
  */
trait StreamingOperator extends Operator with OperatorState {

  final override def nextTasks(context: QueryContext,
                               state: QueryState,
                               operatorInput: OperatorInput,
                               resources: QueryResources): IndexedSeq[ContinuableOperatorTask] = {
    val input = operatorInput.takeMorsel()
    if (input != null) {
      nextTasks(context, state, input, resources)
    } else {
      null
    }
  }

  /**
    * Initialize new tasks for this operator. This code path let's operators create
    * multiple output rows for each row in `inputMorsel`.
    */
  protected def nextTasks(context: QueryContext,
                          state: QueryState,
                          inputMorsel: MorselParallelizer,
                          resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel]

  override final def createState(argumentStateCreator: ArgumentStateMapCreator): OperatorState = this
}

/**
  * A continuable operator is initialized to produce exactly one [[ContinuableOperatorTask]].
  */
trait ContinuableOperator extends HasWorkIdentity {
  def init(context: QueryContext,
           state: QueryState,
           resources: QueryResources): ContinuableOperatorTask
}

trait MiddleOperator extends HasWorkIdentity {
  def createTask(argumentStateCreator: ArgumentStateMapCreator,
                 queryContext: QueryContext,
                 state: QueryState,
                 resources: QueryResources): OperatorTask
}

trait StatelessOperator extends MiddleOperator with OperatorTask {
  final override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                                queryContext: QueryContext,
                                state: QueryState,
                                resources: QueryResources): OperatorTask = this
}

/**
  * Operator related task.
  */
trait OperatorTask {
  def operate(output: MorselExecutionContext,
              context: QueryContext,
              state: QueryState,
              resources: QueryResources): Unit
}

/**
  * Operator task which might require several operate calls to be fully executed.
  */
trait ContinuableOperatorTask extends OperatorTask {
  def canContinue: Boolean
  def close(operatorCloser: OperatorCloser): Unit
  def producingWorkUnitEvent: WorkUnitEvent

  /**
    * Remove everything related to cancelled argumentRowIds from to the task's input.
    *
    * @return `true` if the task has become obsolete.
    */
  def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean
}

trait ContinuableOperatorTaskWithMorsel extends ContinuableOperatorTask {
  val inputMorsel: MorselExecutionContext

  override def close(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeMorsel(inputMorsel)
  }

  override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    operatorCloser.filterCancelledArguments(inputMorsel)
  }

  override def producingWorkUnitEvent: WorkUnitEvent = inputMorsel.producingWorkUnitEvent
}

trait ContinuableOperatorTaskWithAccumulator[ACC <: MorselAccumulator] extends ContinuableOperatorTask {
  val accumulator: ACC

  override def close(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeAccumulator(accumulator)
  }

  override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    operatorCloser.filterCancelledArguments(accumulator)
  }

  override def producingWorkUnitEvent: WorkUnitEvent = null
}

trait ContinuableOperatorTaskWithMorselAndAccumulator[ACC <: MorselAccumulator]
  extends ContinuableOperatorTaskWithMorsel
  with ContinuableOperatorTaskWithAccumulator[ACC] {

  override def close(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeMorselAndAccumulatorTask(inputMorsel, accumulator)
  }

  override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    operatorCloser.filterCancelledArguments(inputMorsel, accumulator)
  }

  override def producingWorkUnitEvent: WorkUnitEvent = inputMorsel.producingWorkUnitEvent
}

abstract class CompiledContinuableOperatorTaskWithMorsel extends ContinuableOperatorTaskWithMorsel {

  override def operate(output: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {
    // For compiled expressions to work they assume that some local variables exists with certain names
    // as defined by the CompiledExpression.evaluate() interface
    operateCompiled(
      //--- Names used by compiled expressions
      output,
      dbAccess = context,
      params = state.params,
      cursors = resources.expressionCursors,
      expressionVariables = resources.expressionVariables(state.nExpressionSlots),
      //--- Additional operator codegen dependencies
      cursorPools = resources.cursorPools,
      //--- Additional produce result codegen dependencies
      resultVisitor = state.visitor,
      prepopulateResults = state.prepopulateResults
    )
  }

  /**
    * The implementation of this method is done by code generation
    *
    * It is currently required to include the same parameter names as CompiledExpression.evaluate()
    * to play well with compiled expressions (where they are defined as static constants).
    * If we expose a way to inject the IntermediateRepresentation for these variable accesses into
    * the expression compiler we can eliminate this abstract class delegation of the operate() call.
    *
    * AnyValue evaluate( ExecutionContext context,
    *                    DbAccess dbAccess,
    *                    AnyValue[] params,
    *                    ExpressionCursors cursors,
    *                    AnyValue[] expressionVariables );
    */
  @throws[Exception]
  def operateCompiled[E <: Exception](context: MorselExecutionContext,
                                      dbAccess: DbAccess,
                                      params: Array[AnyValue],
                                      cursors: ExpressionCursors,
                                      expressionVariables: Array[AnyValue],
                                      cursorPools: CursorPools,
                                      resultVisitor: QueryResultVisitor[E],
                                      prepopulateResults: Boolean): Unit
}
