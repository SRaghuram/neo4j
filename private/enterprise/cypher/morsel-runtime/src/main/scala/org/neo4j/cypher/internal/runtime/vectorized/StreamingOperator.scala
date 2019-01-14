/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.parallel.{HasWorkIdentity, Task, WorkIdentity}

/**
  * Physical immutable operator. [[StreamingOperator#init]] is thread-safe, and creates a [[ContinuableOperatorTask]]
  * which can be executed.
  *
  * Operators are expected to operate in a streaming fashion, where every inputMorsel
  * results in a new task.
  */
trait StreamingOperator extends HasWorkIdentity {
  def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext, resources: QueryResources): IndexedSeq[ContinuableOperatorTask]
}

/**
  * Physical immutable operator. [[StreamingMergeOperator#init]] is thread-safe, and creates two [[ContinuableOperatorTask]]s
  * which can be executed.
  *
  * Operators are expected to operate in a streaming fashion, where every inputMorsel
  * results in a new task.
  *
  * If initFromLhs returns a PipelineArgument the owner StreamingJoinPipeline will take care of scheduling a task from the
  * leaf pipeline of the RHS upstream. The PipelineArgument will be passed to this RHS task, and along subsequent tasks until
  * it eventually arrives in the initFromRhs.
  */
trait StreamingMergeOperator extends HasWorkIdentity {
  def initFromLhs(context: QueryContext, state: QueryState, inputLhsMorsel: MorselExecutionContext, resources: QueryResources):
    (Option[ContinuableOperatorTask], Option[SinglePARG])
  def initFromRhs(context: QueryContext, state: QueryState, inputRhsMorsel: MorselExecutionContext, resources: QueryResources, pipelineArgument: PipelineArgument):
    Option[ContinuableOperatorTask]
  def argumentSize: SlotConfiguration.Size
}

/**
  * Physical immutable operator. [[EagerReduceOperator#init]] is thread-safe, and creates a [[ContinuableOperatorTask]]
  * which can be executed.
  *
  * [[EagerReduceOperator]]s act as a barrier between pipelines, where all input morsels of the upstream pipeline
  * have to be collected before the reduce can start.
  */
trait EagerReduceOperator extends HasWorkIdentity {
  def init(context: QueryContext, state: QueryState, inputMorsels: Seq[MorselExecutionContext], resources: QueryResources): ContinuableOperatorTask
}

/**
  * Physical immutable operator. [[LazyReduceOperator#init]] is thread-safe, and creates a [[ContinuableOperatorTask]]
  * which can be executed.
  */
trait LazyReduceOperator extends HasWorkIdentity {
  /**
    * Create a task that
    * - pulls from the queue and processes the morsels
    * - has a retry loop that calls [[LazyReduceCollector.trySetTaskDone()]]
    */
  def init(context: QueryContext,
           state: QueryState,
           messageQueue: util.Queue[MorselExecutionContext],
           collector: LazyReduceCollector,
           resources: QueryResources): LazyReduceOperatorTask
}

/**
  * Physical immutable operator. Thread-safe. In contrast to [[StreamingOperator]] and [[EagerReduceOperator]], [[StatelessOperator]]
  * has no init-method to generate a task, but performs it's logic directly in the [[StatelessOperator#operate]] call.
  */
trait StatelessOperator extends OperatorTask with HasWorkIdentity

/**
  * Operator related task.
  */
trait OperatorTask {
  def operate(data: MorselExecutionContext, context: QueryContext, state: QueryState, resources: QueryResources): Unit
}

/**
  * Operator task which might require several operate calls to be fully executed.
  */
trait ContinuableOperatorTask extends OperatorTask {
  def canContinue: Boolean
}

/**
  * Streaming operator task which takes an input morsel and produces one or many output rows
  * for each input row, and might require several operate calls to be fully executed.
  */
trait StreamingContinuableOperatorTask extends ContinuableOperatorTask {
  val inputRow: MorselExecutionContext

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

  override def operate(outputRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {

    while ((inputRow.isValidRow || innerLoop) && outputRow.isValidRow) {
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
    inputRow.isValidRow || innerLoop
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

  def acceptMorsel(inputMorsel: MorselExecutionContext,
                   context: QueryContext,
                   state: QueryState,
                   resources: QueryResources,
                   from: AbstractPipelineTask): IndexedSeq[Task[QueryResources]]

  def produceTaskScheduled(): Unit

  def produceTaskCompleted(context: QueryContext,
                           state: QueryState,
                           resources: QueryResources): Seq[Task[QueryResources]]
}
