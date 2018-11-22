/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.parallel.Task

/**
  * Physical immutable operator. [[StreamingOperator#init]] is thread-safe, and creates a [[ContinuableOperatorTask]]
  * which can be executed.
  *
  * Operators are expected to operate in a streaming fashion, where every inputMorsel
  * results in a new task.
  */
trait StreamingOperator {
  def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext, cursors: ExpressionCursors): ContinuableOperatorTask
}

/**
  * Physical immutable operator. [[EagerReduceOperator#init]] is thread-safe, and creates a [[ContinuableOperatorTask]]
  * which can be executed.
  *
  * [[EagerReduceOperator]]s act as a barrier between pipelines, where all input morsels of the upstream pipeline
  * have to be collected before the reduce can start.
  */
trait EagerReduceOperator {
  def init(context: QueryContext, state: QueryState, inputMorsels: Seq[MorselExecutionContext], cursors: ExpressionCursors): ContinuableOperatorTask
}

/**
  * Physical immutable operator. [[LazyReduceOperator#init]] is thread-safe, and creates a [[ContinuableOperatorTask]]
  * which can be executed.
  */
trait LazyReduceOperator {
  /**
    * Create a task that
    * - pulls from the queue and processes the morsels
    * - has a retry loop that calls [[LazyReduceCollector.trySetTaskDone()]]
    */
  def init(context: QueryContext,
           state: QueryState,
           messageQueue: util.Queue[MorselExecutionContext],
           collector: LazyReduceCollector,
           cursors: ExpressionCursors): LazyReduceOperatorTask
}

/**
  * Physical immutable operator. Thread-safe. In contrast to [[StreamingOperator]] and [[EagerReduceOperator]], [[StatelessOperator]]
  * has no init-method to generate a task, but performs it's logic directly in the [[StatelessOperator#operate]] call.
  */
trait StatelessOperator extends OperatorTask

/**
  * Operator related task.
  */
trait OperatorTask {
  def operate(data: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit
}

/**
  * Operator task which might require several operate calls to be fully executed.
  */
trait ContinuableOperatorTask extends OperatorTask {
  def canContinue: Boolean
}

object NOTHING_TO_CLOSE extends AutoCloseable {
  override def close(): Unit = {}
}

/**
  * Streaming operator task which takes an input morsel and produces one or many output rows
  * for each input row, and might require several operate calls to be fully executed.
  */
trait StreamingContinuableOperatorTask extends ContinuableOperatorTask {
  val inputRow: MorselExecutionContext

  protected def initializeInnerLoop(inputRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): AutoCloseable
  protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit

  private var innerLoop: AutoCloseable = _

  override def operate(outputRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       cursors: ExpressionCursors): Unit = {

    while ((inputRow.hasMoreRows || innerLoop != null) && outputRow.hasMoreRows) {
      if (innerLoop == null) {
        innerLoop = initializeInnerLoop(inputRow, context, state, cursors)
      }
      // Do we have any output rows for this input row?
      if (innerLoop != null) {
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
        if (outputRow.hasMoreRows) {
          innerLoop.close()
          innerLoop = null
          inputRow.moveToNextRow()
        }
      }
      else {
        // Nothing to do for this input row, move to the next
        inputRow.moveToNextRow()
      }
    }

    outputRow.finishedWriting()
  }

  override def canContinue: Boolean =
    inputRow.hasMoreRows || innerLoop != null
}

/**
  * A [[ReduceCollector]] holds morsels in front of a [[EagerReduceOperator]]. It relies on reference counting
  * of upstreams tasks in order to know when all expected data has arrived, at which point it will schedule
  * the downstream reduce computation.
  *
  * The contract here is
  *   1) on every upstream scheduling of a task at any level, call [[ReduceCollector#produceTaskScheduled]]
  *   2) every direct upstream task hands over morsels by [[ReduceCollector#acceptMorsel]]
  *   3) on every upstreams task completion (after the final [[ReduceCollector#acceptMorsel]]), call [[ReduceCollector#produceTaskCompleted]]
  *
  * On the final [[ReduceCollector#produceTaskCompleted]] the downstream reduce task will be returned.
  */
trait ReduceCollector {

  def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors,
                   from: AbstractPipelineTask): Option[Task[ExpressionCursors]]

  def produceTaskScheduled(task: String): Unit

  def produceTaskCompleted(task: String, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]]
}
