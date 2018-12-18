/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent._
import java.util.function.Supplier

import scala.concurrent.duration.Duration

class LockFreeScheduler[THREAD_LOCAL_RESOURCE <: AutoCloseable](override val numberOfWorkers: Int,
                                                                waitTimeout: Duration,
                                                                threadLocalResourceFactory: () => THREAD_LOCAL_RESOURCE
                                                               ) extends Scheduler[THREAD_LOCAL_RESOURCE] {

  private val tasks = new ConcurrentLinkedQueue[Callable[TaskResult]]()

  private val threadLocalResource = ThreadLocal.withInitial(new Supplier[THREAD_LOCAL_RESOURCE] {
    override def get(): THREAD_LOCAL_RESOURCE = threadLocalResourceFactory()
  })

  // Kick off all the workers in the beginning
  for (_ <- 0 until numberOfWorkers) {
    val worker = new Worker()
    new Thread(worker).start()
  }

  override def isMultiThreaded: Boolean = true

  override def execute(tracer: SchedulerTracer, tasks: IndexedSeq[Task[THREAD_LOCAL_RESOURCE]]): QueryExecution = {
    val queryTracer: QueryExecutionTracer = tracer.traceQuery()

    val queryExecution = new LockFreeQueryExecution(queryTracer)

    // schedule the first tasks
    tasks.foreach(schedule(_, queryExecution, None))

    queryExecution
  }

  private def schedule(task: Task[THREAD_LOCAL_RESOURCE],
                       queryExecution: LockFreeQueryExecution,
                       upstreamWorkUnit: Option[WorkUnitEvent]): Unit = {
    // Schedule in the query
    queryExecution.schedule(task, upstreamWorkUnit)
      // Schedule in the global queue
      .foreach(tasks.add)
  }

  class Worker extends Runnable {
    override def run(): Unit = {
      while (true) {
        // Grep the first available task
        val task = tasks.poll()
        if (task != null) {
          // Execute the task
          var taskResult: TaskResult = null
          try {
            taskResult = task.call() // TODO time out `call`
          } catch {
            case _: TimeoutException =>
              // got tired of waiting to complete, put it back into the queue
              // TODO we should call `abort` or similar on the workUnitEvent of the tracer
              tasks.add(task)
            case _: QueryAbortedException =>
            // Do we need to do anything here?
          }

          // Abort and retry if we got an exception.
          if (taskResult != null) {
            // Schedule all resulting new tasks
            for (newTask <- taskResult.newDownstreamTasks)
              schedule(newTask, taskResult.queryExecution, Some(taskResult.workUnitEvent))
            if (taskResult.task.canContinue) {
              schedule(taskResult.task, taskResult.queryExecution, Some(taskResult.workUnitEvent))
            }

            // Mark as done in the query
            taskResult.queryExecution.done(task)
          }
        } else {
          // TODO sleep?
        }
      }
    }
  }

  class LockFreeQueryExecution(queryTracer: QueryExecutionTracer) extends QueryExecution {
    private val latch = new CountDownLatch(1)
    private val scheduledTasks = new ConcurrentLinkedQueue[Callable[TaskResult]]()
    @volatile
    private var queryFailed: Option[Throwable] = None

    def schedule(task: Task[THREAD_LOCAL_RESOURCE],
                 upstreamWorkUnit: Option[WorkUnitEvent]): Option[Callable[TaskResult]] = {
      if (queryFailed.isDefined) {
        // The query failed. Thus we don't accept any new tasks
        return None
      }

      val scheduledWorkUnitEvent = queryTracer.scheduleWorkUnit(task, upstreamWorkUnit)

      val callableTask =
        new Callable[TaskResult] {
          override def call(): TaskResult = {
            // Don't bother starting if the query failed in the meantime
            if (queryFailed.isDefined) {
              throw new QueryAbortedException(null)
            }

            val workUnitEvent = scheduledWorkUnitEvent.start()
            try {
              val newDownstreamTasks = task.executeWorkUnit(threadLocalResource.get())
              TaskResult(task, LockFreeQueryExecution.this, workUnitEvent, newDownstreamTasks)
            } catch {
              case e: TimeoutException => // Handled by the worker. Rethrow
                throw e
              case t: Throwable => // Stop the query immediately
                stop(Some(t))
                throw new QueryAbortedException(t)
            } finally {
              workUnitEvent.stop()
            }
          }
        }
      // Mark as scheduled
      scheduledTasks.add(callableTask)
      Some(callableTask)
    }

    private def stop(result: Option[Throwable]): Unit = {
      queryFailed = result
      queryTracer.stopQuery()
      latch.countDown()
    }

    def done(callableTask: Callable[TaskResult]): Unit = {
      scheduledTasks.remove(callableTask)
      if (scheduledTasks.isEmpty) {
        stop(None)
      }
    }

    override def await(): Option[Throwable] = {
      latch.await()
      queryFailed
    }
  }

  class QueryAbortedException(cause: Throwable) extends Exception(cause)

  case class TaskResult(task: Task[THREAD_LOCAL_RESOURCE],
                        queryExecution: LockFreeQueryExecution,
                        workUnitEvent: WorkUnitEvent,
                        newDownstreamTasks: Seq[Task[THREAD_LOCAL_RESOURCE]])

}
