/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import org.neo4j.cypher.internal.runtime.scheduling.{QueryExecutionTracer, ScheduledWorkUnitEvent, SchedulerTracer, Task, WorkUnitEvent}

/**
  * This class simply prints information to stdout. It uses `print` instead of `println`
  * to avoid any synchronization between Threads as part of tracing. This can be useful
  * to uncover concurrency issues that would disappear with a Tracer that synchronizes
  * between Threads.
  */
class PrintSchedulerTracer() extends SchedulerTracer {
  override def traceQuery(): QueryExecutionTracer = QueryTracer()

  case class QueryTracer() extends QueryExecutionTracer {
    print(s"START $this\n")

    private final val NO_UPSTREAM : Long = -1

    override def scheduleWorkUnit(task: Task[_ <: AutoCloseable], upstreamWorkUnit: Option[WorkUnitEvent]): ScheduledWorkUnitEvent = {
      val schedulingThread = Thread.currentThread().getId
      val upstreamWorkUnitId = upstreamWorkUnit.map(_.id).getOrElse(NO_UPSTREAM)
      val swu = ScheduledWorkUnit(upstreamWorkUnitId, schedulingThread, task)
      print(s"SCHEDULE $swu\n")
      swu
    }

    override def stopQuery(): Unit = {

      print(s"STOP $this\n")
    }
  }

  case class ScheduledWorkUnit(upstreamWorkUnitId: Long,
                               schedulingThreadId: Long,
                               task: Task[_ <: AutoCloseable]) extends ScheduledWorkUnitEvent {

    override def start(): WorkUnitEvent = {

      val wu = WorkUnit(upstreamWorkUnitId,
          schedulingThreadId,
          Thread.currentThread().getId,
          task)
      print(s"START $wu\n")
      wu
    }
  }

  case class WorkUnit(upstreamId: Long,
                      schedulingThreadId: Long,
                      executionThreadId: Long,
                      task: Task[_ <: AutoCloseable]) extends WorkUnitEvent {

    override def id: Long = 1

    override def stop(): Unit = {
      print(s"STOP $this\n")
    }
  }
}
