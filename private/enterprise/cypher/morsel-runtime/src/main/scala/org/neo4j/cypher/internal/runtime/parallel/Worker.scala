/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent.Callable
import java.util.concurrent.TimeoutException

class Worker[TASK_RESULT](schedulerClient: SchedulerClient[TASK_RESULT]) extends Runnable {
  override def run(): Unit = {
    while (true) {
      // Grep the first available task
      val task = schedulerClient.nextTask()
      if (task != null) {
        // Execute the task
        var taskResult: TASK_RESULT = null.asInstanceOf[TASK_RESULT]
        try {
          taskResult = task.call() // TODO time out `call`
        } catch {
          case _: TimeoutException =>
          // got tired of waiting to complete, put it back into the queue
          // TODO we should call `abort` or similar on the workUnitEvent of the tracer
          //tasks.add(task)
          case _: QueryAbortedException =>
          // Do we need to do anything here?
        }

        if (taskResult != null) {
          // Mark the task as done
          schedulerClient.taskDone(taskResult)
        }
      } else {
        // TODO sleep?
      }
    }
  }
}

trait SchedulerClient[TASK_RESULT] {
  /**
    * Obtain the next task to be worked on.
    */
  def nextTask(): Callable[TASK_RESULT]

  /**
    * Called when the [[Worker]] completed its work on the task.
    */
  def taskDone(taskResult: TASK_RESULT): Unit
}

class QueryAbortedException(cause: Throwable) extends Exception(cause)