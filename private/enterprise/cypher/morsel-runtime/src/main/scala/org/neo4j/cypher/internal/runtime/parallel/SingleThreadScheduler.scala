/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.dprintln

import scala.collection.mutable

/**
  * Single threaded implementation of the Scheduler trait
  */
class SingleThreadScheduler[T <: AutoCloseable](threadLocalResourceFactory: () => T) extends Scheduler[T] {

  override def execute(tracer: SchedulerTracer, tasks: IndexedSeq[Task[T]]): QueryExecution = {
    dprintln(() => s"SingleThreadScheduler execute $tasks")
    new SingleThreadQueryExecution(tasks, tracer.traceQuery())
  }

  override def numberOfWorkers: Int = 1

  class SingleThreadQueryExecution(initialTasks: IndexedSeq[Task[T]], tracer: QueryExecutionTracer) extends QueryExecution {

    private val jobStack: mutable.Stack[(Task[T],ScheduledWorkUnitEvent)] = new mutable.Stack()
    initialTasks.foreach(schedule(_, None))

    override def await(): Option[Throwable] = {

      val threadLocalResource = threadLocalResourceFactory()

      val result = try {
        while (jobStack.nonEmpty) {
          val nextTaskAndEvent = jobStack.pop()
          val nextTask = nextTaskAndEvent._1
          val nextTaskScheduledEvent = nextTaskAndEvent._2

          dprintln(() => s"SingleThreadedScheduler running $nextTask")

          val workUnitEvent = nextTaskScheduledEvent.start()
          val downstreamTasks =
            try {
              nextTask.executeWorkUnit(threadLocalResource)
            } finally {
              workUnitEvent.stop()
            }

          if (nextTask.canContinue)
            schedule(nextTask, Some(workUnitEvent))

          for (newTask <- downstreamTasks)
            schedule(newTask, Some(workUnitEvent))
        }
        None
      } catch {
        case t: Throwable => Some(t)
      } finally {
        threadLocalResource.close()
      }
      tracer.stopQuery()
      result
    }

    private def schedule(task: Task[T], upstreamWorkUnitEvent: Option[WorkUnitEvent]): Unit = {
      dprintln(() => s"SingleThreadedScheduler schedule $task")
      val scheduledWorkUnitEvent = tracer.scheduleWorkUnit(task, upstreamWorkUnitEvent)
      jobStack.push((task,scheduledWorkUnitEvent))
    }
  }
}
