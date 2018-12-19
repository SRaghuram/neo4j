/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch

import org.neo4j.cypher.internal.runtime.parallel.LockFreeScheduler.TaskResult

class LockFreeQueryExecution[THREAD_LOCAL_RESOURCE <: AutoCloseable](queryTracer: QueryExecutionTracer, threadLocalResource: ThreadLocal[THREAD_LOCAL_RESOURCE])
  extends QueryExecution
  with QuerySchedulerClient[THREAD_LOCAL_RESOURCE, TaskResult[THREAD_LOCAL_RESOURCE]] {

  private val latch = new CountDownLatch(1)
  private val scheduledTasks = new ConcurrentLinkedQueue[Callable[TaskResult[THREAD_LOCAL_RESOURCE]]]()
  @volatile
  private var queryFailed: Option[Throwable] = None

  override def makeCallableTask(task: Task[THREAD_LOCAL_RESOURCE],
                                upstreamWorkUnit: Option[WorkUnitEvent]): Option[Callable[TaskResult[THREAD_LOCAL_RESOURCE]]] = {
    if (queryFailed.isDefined) {
      // The query failed. Thus we don't accept any new tasks
      return None
    }
    val scheduledWorkUnitEvent = queryTracer.scheduleWorkUnit(task, upstreamWorkUnit)

    val callableTask =
      new Callable[TaskResult[THREAD_LOCAL_RESOURCE]] {
        override def call(): TaskResult[THREAD_LOCAL_RESOURCE] = {
          // Don't bother starting if the query failed in the meantime
          if (queryFailed.isDefined) {
            throw new QueryAbortedException(null)
          }

          val workUnitEvent = scheduledWorkUnitEvent.start()
          try {
            val newDownstreamTasks = task.executeWorkUnit(threadLocalResource.get())
            val result = TaskResult(task, this, LockFreeQueryExecution.this, workUnitEvent, newDownstreamTasks)
            workUnitEvent.stop()
            result
          } catch {
            case t: Throwable => // Stop the query immediately
              workUnitEvent.stop()
              stop(Some(t))
              throw new QueryAbortedException(t)
          }
        }
      }
    Some(callableTask)
  }

  override def taskScheduled(callableTask: Callable[TaskResult[THREAD_LOCAL_RESOURCE]]): Unit = {
    scheduledTasks.add(callableTask)
  }

  override def taskDone(taskResult: TaskResult[THREAD_LOCAL_RESOURCE]): Unit = {
    scheduledTasks.remove(taskResult.callableTask)
    if (scheduledTasks.isEmpty) {
      stop(None)
    }
  }

  override def await(): Option[Throwable] = {
    latch.await()
    queryFailed
  }

  private def stop(result: Option[Throwable]): Unit = {
    queryFailed = result
    queryTracer.stopQuery()
    latch.countDown()
  }
}
