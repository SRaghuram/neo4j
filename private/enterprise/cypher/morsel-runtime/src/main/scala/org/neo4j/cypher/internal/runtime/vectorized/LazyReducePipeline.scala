/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.{Task, WorkIdentity}
import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.dprintln

/**
  * A pipeline that is fed with input morsels asynchronously into a queue,
  * and at the same time processes these morsels in a [[ContinuableOperatorTask]].
  *
  * The difference to a [[EagerReducePipeline]] is that this Pipeline starts its work
  * as soon as the first input morsel arrives.
  *
  * The difference to a [[StreamingPipeline]] is that this Pipeline is stateless,
  * i.e. the start operator does not need to write to new morsels. Another difference
  * is that the [[LazyReduceOperatorTask]]s can never be run in parallel, while [[ContinuableOperatorTask]]s can.
  */
class LazyReducePipeline(start: LazyReduceOperator,
                         override val slots: SlotConfiguration,
                         override val upstream: Option[Pipeline]) extends ReducePipeline {

  override val workIdentity: WorkIdentity = composeWorkIdentities(start, operators)

  override def acceptMorsel(inputMorsel: MorselExecutionContext,
                            context: QueryContext,
                            state: QueryState,
                            cursors: ExpressionCursors,
                            pipelineArgument: PipelineArgument,
                            from: AbstractPipelineTask): Seq[Task[ExpressionCursors]] = {
    state.reduceCollector.get.acceptMorsel(inputMorsel, context, state, cursors, from)
  }

  override def initCollector(): Collector = new Collector

  class Collector() extends LazyReduceCollector {

    private val queue = new ConcurrentLinkedQueue[MorselExecutionContext]()
    private val taskCount = new AtomicInteger(0)
    /**
      * This state will be atomically updated
      * - in [[acceptMorsel]], from any of the upstream pipeline's tasks
      * - in [[trySetTaskDone]], from this pipeline's task
      */
    private val reduceTaskState = new AtomicReference[ReduceTaskState](NoTaskScheduled)

    override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors,
                              from: AbstractPipelineTask): Seq[Task[ExpressionCursors]] = {
      // First we put the next morsel in the queue
      queue.add(inputMorsel)

      // Try updating the state in a loop, until it succeeds.
      var updatedState = false
      var maybeNextTask: Option[LazyReduceOperatorTask] = None
      while (!updatedState) {
        reduceTaskState.get() match {
          case NoTaskScheduled =>
            // We have to try and schedule the first task
            val firstTask = start.init(context, state, queue, this, cursors)
            maybeNextTask = Some(firstTask)
            updatedState = reduceTaskState.compareAndSet(NoTaskScheduled, ScheduledAndGoing(firstTask, 1))
          case s@ScheduledAndGoing(reduceTask, morselsArrived) =>
            // We don't have to schedule a new task, the running task will pick up the new morsel in the queue
            maybeNextTask = None
            updatedState = reduceTaskState.compareAndSet(s, ScheduledAndGoing(reduceTask, morselsArrived + 1))
        }
      }

      maybeNextTask.map { reduceTask =>
        val nextState = initDownstreamReduce(state)
        produceTaskScheduledForReduceCollector(nextState)
        LazyReducePipelineTask(
          reduceTask,
          operators,
          slots,
          workIdentity,
          context,
          nextState,
          from.pipelineArgument,
          from.ownerPipeline,
          downstream)
      }.toSeq
    }

    override def trySetTaskDone(task: LazyReduceOperatorTask, morselsProcessed: Int): Boolean = {
      reduceTaskState.get() match {
        case s@ScheduledAndGoing(`task`, `morselsProcessed`) =>
          // Mark the task as done
          reduceTaskState.compareAndSet(s, NoTaskScheduled)
        case ScheduledAndGoing(`task`, _) =>
          // Some other morsel arrived in the meantime. Now we have to process that as well
          false
        case s =>
          throw new IllegalStateException(s"Trying to set a task ($task) done, but had unexpected state: $s")
      }
    }

    override def produceTaskScheduled(): Unit = {
      val tasks = taskCount.incrementAndGet()
      if (Pipeline.DEBUG) {
        dprintln(() => "taskCount [%3d]: scheduled %s".format(tasks, toString))
      }
    }

    override def produceTaskCompleted(context: QueryContext, state: QueryState, cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {
      val tasksLeft = taskCount.decrementAndGet()
      if (Pipeline.DEBUG) {
        dprintln(() => "taskCount [%3d]: completed %s".format(tasksLeft, toString))
      }

      if (tasksLeft >= 0) {
        Seq.empty
      } else {
        throw new IllegalStateException("Reference counting of tasks has failed: now at task count " + tasksLeft)
      }
    }
  }

}

trait LazyReduceCollector extends ReduceCollector {
  /**
    * Executed from the [[LazyReduceOperatorTask]]s Thread. Tries to mark itself as done. If that fails,
    * due to concurrently arriving new data, the Task has to continue instead.
    *
    * This method has to be called from a retry loop, that, if false is returned, processes any new items
    * in the queue, and then retries setting itself done.
    *
    * @param task the [[LazyReduceOperatorTask]]
    * @return if successfully set to done
    */
  def trySetTaskDone(task: LazyReduceOperatorTask, morselsProcessed: Int): Boolean
}

/**
  * Atomically updated states for the Collector. Beware that these are compared by reference equality!
  */
sealed trait ReduceTaskState

/**
  * No reduce task is currently scheduled.
  */
case object NoTaskScheduled extends ReduceTaskState

/**
  * A reduce task has been scheduled. It still has unprocessed data.
  *
  * @param reduceTask     the task.
  * @param morselsArrived how many morsels arrived.
  */
case class ScheduledAndGoing(reduceTask: LazyReduceOperatorTask, morselsArrived: Int) extends ReduceTaskState
