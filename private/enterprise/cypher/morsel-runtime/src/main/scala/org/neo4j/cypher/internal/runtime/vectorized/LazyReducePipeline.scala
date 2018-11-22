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
import org.neo4j.cypher.internal.runtime.parallel.Task

/**
  * A pipeline that is fed with input morsels asynchronously into a queue,
  * and at the same time processes these morsels in a [[ContinuableOperatorTask]].
  *
  * The difference to a [[EagerReducePipeline]] is that this Pipeline starts its work
  * as soon as the first input morsel arrives.
  *
  * The difference to a [[StreamingPipeline]] is that this Pipeline executes all work
  * in one [[ContinuableOperatorTask]], not multiple.
  */
class LazyReducePipeline(start: LazyReduceOperator,
                         override val slots: SlotConfiguration,
                         override val upstream: Option[Pipeline]) extends ReducePipeline {

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"LazyReducePipeline(${x.mkString(",")})"
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
    state.reduceCollector.get.acceptMorsel(inputMorsel, context, state, cursors)
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
    private val reduceTaskState = new AtomicReference[ReduceTaskState](NotScheduled)

    override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
      // First we put the next morsel in the queue
      queue.add(inputMorsel)

      // Try updating the state in a loop, until it succeeds.
      var updatedState = false
      var maybeNextTask: Option[ContinuableOperatorTask] = None
      while(!updatedState) {
        reduceTaskState.get() match {
          case NotScheduled =>
            // We have to try and schedule the first task
            val firstTask = start.init(context, state, queue, this, cursors)
            maybeNextTask = Some(firstTask)
            updatedState = reduceTaskState.compareAndSet(NotScheduled, ScheduledAndGoing(firstTask, 1))
          case s:ScheduledAndDone =>
            // We have to try and schedule a new Task
            val nextTask = start.init(context, state, queue, this, cursors)
            maybeNextTask = Some(nextTask)
            updatedState = reduceTaskState.compareAndSet(s, ScheduledAndGoing(nextTask, 1))
          case s@ScheduledAndGoing(reduceTask, morselsArrived) =>
            // We don't have to schedule a new task, the running task will pick up the new morsel in the queue
            maybeNextTask = None
            updatedState = reduceTaskState.compareAndSet(s, ScheduledAndGoing(reduceTask, morselsArrived + 1))
        }
      }

      maybeNextTask.map { reduceTask =>
        val nextState = initDownstreamReduce(state)
        pipelineTask(reduceTask, context, nextState)
      }
    }

    override def trySetTaskDone(task: ContinuableOperatorTask, morselsProcessed: Int): Boolean = {
      reduceTaskState.get() match {
        case s@ScheduledAndGoing(`task`, `morselsProcessed`) =>
          // Mark the task as done
          reduceTaskState.compareAndSet(s, ScheduledAndDone(task))
        case ScheduledAndGoing(`task`, _) =>
          // Some other morsel arrived in the meantime. Now we have to process that as well
          false
        case s =>
          throw new IllegalStateException(s"Trying to set a task ($task) done, but it is not even Scheduled or already done: $s")
      }
    }

    override def produceTaskScheduled(task: String): Unit = {
      val tasks = taskCount.incrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: scheduled %s".format(tasks, task))
    }

    override def produceTaskCompleted(task: String, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
      val tasksLeft = taskCount.decrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: completed %s".format(tasksLeft, task))

      if (tasksLeft >= 0) {
        None
      } else {
        throw new IllegalStateException("Reference counting of tasks has failed: now at task count " + tasksLeft)
      }
    }
  }

}

trait LazyReduceCollector extends ReduceCollector {
  /**
    * Executed from the [[ContinuableOperatorTask]]s Thread. Tries to mark itself as done. If that fails,
    * due to concurrently arriving new data, the Task has to continue instead.
    *
    * This method has to be called from a retry loop, that, if false is returned, processed any new items
    * in the queue, and then retries setting itself done.
    *
    * @param task the [[ContinuableOperatorTask]]
    * @return if successfully set to done
    */
  def trySetTaskDone(task: ContinuableOperatorTask, morselsProcessed: Int): Boolean
}

/**
  * Atomically updated states for the Collector. Beware that these are compared by reference equality!
  */
sealed trait ReduceTaskState

/**
  * No reduce task has been scheduled yet.
  */
case object NotScheduled extends ReduceTaskState

/**
  * A reduce task has been scheduled. It still has unprocessed data.
  *
  * @param reduceTask the task.
  * @param morselsArrived how many morsels arrived.
  */
case class ScheduledAndGoing(reduceTask: ContinuableOperatorTask, morselsArrived: Int) extends ReduceTaskState

/**
  * A reduce task has been scheduled. It has processed all currently available data.
  *
  * @param reduceTask the task.
  */
case class ScheduledAndDone(reduceTask: ContinuableOperatorTask) extends ReduceTaskState
