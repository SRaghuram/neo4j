/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import scala.collection.mutable

/**
  * Single threaded implementation of the Scheduler trait
  */
class SingleThreadScheduler() extends Scheduler {

  override def execute(task: Task, tracer: SchedulerTracer): QueryExecution =
    new SingleThreadQueryExecution(task, tracer.traceQuery())

  def isMultiThreaded: Boolean = false

  class SingleThreadQueryExecution(initialTask: Task, tracer: QueryExecutionTracer) extends QueryExecution {

    private val jobStack: mutable.Stack[(Task,ScheduledWorkUnitEvent)] = new mutable.Stack()
    schedule(initialTask, None)

    override def await(): Option[Throwable] = {

      try {
        while (jobStack.nonEmpty) {
          val nextTaskAndEvent = jobStack.pop()
          val nextTask = nextTaskAndEvent._1
          val nextTaskScheduledEvent = nextTaskAndEvent._2

          val workUnitEvent = nextTaskScheduledEvent.start()
          val downstreamTasks =
            try {
              nextTask.executeWorkUnit()
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
      }
    }

    private def schedule(task: Task, upstreamWorkUnitEvent: Option[WorkUnitEvent]) = {
      val scheduledWorkUnitEvent = tracer.scheduleWorkUnit(task, upstreamWorkUnitEvent)
      jobStack.push((task,scheduledWorkUnitEvent))
    }
  }
}
