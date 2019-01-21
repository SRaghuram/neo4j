/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.scheduling

import java.util.concurrent._
import java.util.function.Supplier

import org.neo4j.cypher.internal.runtime.scheduling.LockFreeScheduler.TaskResult

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
  * This Scheduler uses a lock-free queue to store the tasks in.
  * All Worker threads do some cooperative scheduling, thus there is
  * no need to have a dedicated Scheduler Thread.
  */
class LockFreeScheduler[THREAD_LOCAL_RESOURCE <: AutoCloseable](threadFactory: ThreadFactory,
                                                                override val numberOfWorkers: Int,
                                                                waitTimeout: Duration,
                                                                threadLocalResourceFactory: () => THREAD_LOCAL_RESOURCE
                                                               ) extends Scheduler[THREAD_LOCAL_RESOURCE] {

  private val tasks = new ConcurrentLinkedQueue[Callable[TaskResult[THREAD_LOCAL_RESOURCE]]]()

  private val threadLocalResource = ThreadLocal.withInitial(new Supplier[THREAD_LOCAL_RESOURCE] {
    override def get(): THREAD_LOCAL_RESOURCE = threadLocalResourceFactory()
  })

  val schedulerClient: SchedulerClient[TaskResult[THREAD_LOCAL_RESOURCE]] = new SchedulerClient[TaskResult[THREAD_LOCAL_RESOURCE]] {

    override def nextTask(): Callable[TaskResult[THREAD_LOCAL_RESOURCE]] = {
      tasks.poll()
    }

    override def taskDone(taskResult: TaskResult[THREAD_LOCAL_RESOURCE]): Unit = {
      val newTasks = new ArrayBuffer[Callable[TaskResult[THREAD_LOCAL_RESOURCE]]]()
      newTasks ++= taskResult.newDownstreamTasks.flatMap(taskResult.query.makeCallableTask(_, Some(taskResult.workUnitEvent)))
      if (taskResult.task.canContinue) {
        newTasks ++= taskResult.query.makeCallableTask(taskResult.task, Some(taskResult.workUnitEvent))
      }
      scheduleTasks(newTasks, taskResult.query)
      taskResult.query.taskDone(taskResult)
    }
  }

  // Kick off all the workers in the beginning
  for (_ <- 0 until numberOfWorkers) {
    val worker = new Worker(schedulerClient)
    threadFactory.newThread(worker).start()
  }

  override def isMultiThreaded: Boolean = true

  override def execute(tracer: SchedulerTracer, tasks: IndexedSeq[Task[THREAD_LOCAL_RESOURCE]]): QueryExecution = {
    val queryExecution = new LockFreeQueryExecution(tracer.traceQuery(), threadLocalResource)

    // schedule the first tasks
    val initialTasks = tasks.flatMap(queryExecution.makeCallableTask(_, None))
    scheduleTasks(initialTasks, queryExecution)

    queryExecution
  }

  /**
    * Simultaneously schedules a set of tasks. Makes sure that all tasks are first marked as scheduled before
    * actually putting them in the queue. This makes sure that the global count of scheduled tasks
    * does not reach 0 too early.
    * @param callables the callables of the tasks to schedule
    */
  private def scheduleTasks(callables: IndexedSeq[Callable[TaskResult[THREAD_LOCAL_RESOURCE]]],
                           query: QuerySchedulerClient[THREAD_LOCAL_RESOURCE, TaskResult[THREAD_LOCAL_RESOURCE]]): Unit = {
    // Schedule in the query

    callables.foreach { callable =>
      // Mark as scheduled.
      // This has to happen before putting any of the callables into the actual queue.
      // Otherwise another worker can complete a task before all of them have been marked as scheduled
      query.taskScheduled(callable)
    }
    callables.foreach { callable =>
      // Schedule in the global queue
      tasks.add(callable)
    }
  }
}

object LockFreeScheduler {
  case class TaskResult[THREAD_LOCAL_RESOURCE <: AutoCloseable](task: Task[THREAD_LOCAL_RESOURCE],
                        callableTask: Callable[TaskResult[THREAD_LOCAL_RESOURCE]],
                        query: QuerySchedulerClient[THREAD_LOCAL_RESOURCE, TaskResult[THREAD_LOCAL_RESOURCE]],
                        workUnitEvent: WorkUnitEvent,
                        newDownstreamTasks: Seq[Task[THREAD_LOCAL_RESOURCE]])
}

trait QuerySchedulerClient[THREAD_LOCAL_RESOURCE <: AutoCloseable, TASK_RESULT] {
  /**
    * Given a task that can be run (i.e. it [[Task.canContinue]])),
    * create a [[Callable]] that will execute the next Continuation.
    */
  def makeCallableTask(task: Task[THREAD_LOCAL_RESOURCE],
                       upstreamWorkUnit: Option[WorkUnitEvent]): Option[Callable[TASK_RESULT]]

  /**
    * Called when the [[LockFreeScheduler]] decides to schedule a task.
    */
  def taskScheduled(callableTask: Callable[TASK_RESULT]): Unit

  /**
    * Called when a task is completed.
    */
  def taskDone(taskResult: TASK_RESULT): Unit
}
