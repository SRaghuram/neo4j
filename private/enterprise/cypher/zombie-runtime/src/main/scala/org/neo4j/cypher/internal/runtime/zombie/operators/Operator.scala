/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, WorkUnitEvent}
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateCreator, ArgumentStateMap, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.{DbAccess, ExpressionCursors, QueryContext}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.internal.kernel.api.NodeCursor
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
    * Take the next input accumulators
    *
    * @return the input accumulators, or `null` if no input is available
    */
  def takeAccumulators[ACC <: MorselAccumulator](argumentStateMap: ArgumentStateMap[ACC]): Iterable[ACC]
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
  def closeAccumulators[ACC <: MorselAccumulator](accumulators: Iterable[ACC]): Unit

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    *
    * @return `true` if the morsel is completely empty after cancellations
    */
  def filterCancelledArguments(morsel: MorselExecutionContext): Boolean

  /**
    * Remove all accumulators related to cancelled argumentRowIds from `accumulators`.
    *
    * @return `true` if accumultors is completely empty after cancellations
    */
  def filterCancelledArguments[ACC <: MorselAccumulator](accumulators: Iterable[ACC]): Boolean
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
  def createState(argumentStateCreator: ArgumentStateCreator): OperatorState
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

  def argumentStateMap: ArgumentStateMap[ACC]

  final override def nextTasks(context: QueryContext,
                               state: QueryState,
                               operatorInput: OperatorInput,
                               resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[ACC]] = {
    val input = operatorInput.takeAccumulators(argumentStateMap)
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
                input: Iterable[ACC],
                resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[ACC]]
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

  override final def createState(argumentStateCreator: ArgumentStateCreator): OperatorState = this
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
  def createState(argumentStateCreator: ArgumentStateCreator,
                  queryContext: QueryContext,
                  state: QueryState,
                  resources: QueryResources): OperatorTask
}

trait StatelessOperator extends MiddleOperator with OperatorTask {
  final override def createState(argumentStateCreator: ArgumentStateCreator,
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
  def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean
}

/**
  * ContinuableOperatorTask with a morsel as input.
  */
trait ContinuableOperatorTaskWithMorsel extends ContinuableOperatorTask {
  val inputMorsel: MorselExecutionContext

  override final def close(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeMorsel(inputMorsel)
  }

  override final def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    operatorCloser.filterCancelledArguments(inputMorsel)
  }

  override def producingWorkUnitEvent: WorkUnitEvent = inputMorsel.producingWorkUnitEvent
}

abstract class CompiledContinuableOperatorTaskWithMorsel extends ContinuableOperatorTaskWithMorsel {
  protected val dataRead: org.neo4j.internal.kernel.api.Read

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
      nodeCursorPool = resources.cursorPools.nodeCursorPool,
      //--- Additional produce result codegen dependencies
      resultVisitor = state.visitor
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
                                      nodeCursorPool: CursorPool[NodeCursor],
                                      resultVisitor: QueryResultVisitor[E]): Unit
}

/**
  * ContinuableOperatorTask with a morsel as input.
  */
trait ContinuableOperatorTaskWithAccumulators[ACC <: MorselAccumulator] extends ContinuableOperatorTask {
  val accumulators: Iterable[ACC]

  override final def close(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeAccumulators(accumulators)
  }

  override final def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    operatorCloser.filterCancelledArguments(accumulators)
  }

  override def producingWorkUnitEvent: WorkUnitEvent = null
}
