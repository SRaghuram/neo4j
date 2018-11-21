/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

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
  * Physical immutable operator. [[ReduceOperator#init]] is thread-safe, and creates a [[ContinuableOperatorTask]]
  * which can be executed.
  *
  * ReduceOperators operate in a blocking fashion, where all input morsels have to be collected
  * upfront and then provided to the operator in one collection.
  */
trait ReduceOperator {
  def init(context: QueryContext, state: QueryState, inputMorsels: Seq[MorselExecutionContext], cursors: ExpressionCursors): ContinuableOperatorTask
}

/**
  * Physical immutable operator. Thread-safe. In contrast to [[StreamingOperator]] and [[ReduceOperator]], [[StatelessOperator]]
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

/**
  * A [[ReduceCollector]] holds morsels in front of a [[ReduceOperator]]. It relies on reference counting
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

  def acceptMorsel(inputMorsel: MorselExecutionContext): Option[Task[ExpressionCursors]]

  def produceTaskScheduled(task: String): Unit

  def produceTaskCompleted(task: String, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]]
}
