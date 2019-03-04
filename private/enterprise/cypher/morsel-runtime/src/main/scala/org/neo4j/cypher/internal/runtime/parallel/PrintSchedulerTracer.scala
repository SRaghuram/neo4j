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

    override def scheduleWorkUnit(task: Task[_ <: AutoCloseable], upstreamWorkUnits: Seq[WorkUnitEvent]): ScheduledWorkUnitEvent = {
      val schedulingThread = Thread.currentThread().getId
      val upstreamWorkUnitIds = upstreamWorkUnits.map(_.id)
      val swu = ScheduledWorkUnit(upstreamWorkUnitIds, schedulingThread, task)
      print(s"SCHEDULE $swu\n")
      swu
    }

    override def stopQuery(): Unit = {

      print(s"STOP $this\n")
    }
  }

  case class ScheduledWorkUnit(upstreamWorkUnitIds: Seq[Long],
                               schedulingThreadId: Long,
                               task: Task[_ <: AutoCloseable]) extends ScheduledWorkUnitEvent {

    override def start(): WorkUnitEvent = {

      val wu = WorkUnit(upstreamWorkUnitIds,
          schedulingThreadId,
          Thread.currentThread().getId,
          task)
      print(s"START $wu\n")
      wu
    }
  }

  case class WorkUnit(upstreamIds: Seq[Long],
                      schedulingThreadId: Long,
                      executionThreadId: Long,
                      task: Task[_ <: AutoCloseable]) extends WorkUnitEvent {

    override def id: Long = 1

    override def stop(): Unit = {
      print(s"STOP $this\n")
    }
  }
}
