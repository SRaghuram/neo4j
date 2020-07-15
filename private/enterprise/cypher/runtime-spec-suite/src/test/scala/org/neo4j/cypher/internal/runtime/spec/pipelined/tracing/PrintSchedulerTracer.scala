/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined.tracing

import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.ScheduledWorkUnitEvent
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

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

    override def scheduleWorkUnit(workId: WorkIdentity, upstreamWorkUnit: WorkUnitEvent): ScheduledWorkUnitEvent = {
      val schedulingThread = Thread.currentThread().getId
      val upstreamWorkUnitIds = Option(upstreamWorkUnit).map(_.id).toList
      val swu = ScheduledWorkUnit(upstreamWorkUnitIds, schedulingThread, workId)
      print(s"SCHEDULE $swu\n")
      swu
    }

    override def stopQuery(): Unit = {

      print(s"STOP $this\n")
    }
  }

  case class ScheduledWorkUnit(upstreamWorkUnitIds: Seq[Long],
                               schedulingThreadId: Long,
                               workId: WorkIdentity) extends ScheduledWorkUnitEvent {

    override def start(): WorkUnitEvent = {

      val wu = WorkUnit(
        upstreamWorkUnitIds,
        schedulingThreadId,
        Thread.currentThread().getId,
        workId)
      print(s"START $wu\n")
      wu
    }
  }

  case class WorkUnit(upstreamIds: Seq[Long],
                      schedulingThreadId: Long,
                      executionThreadId: Long,
                      workId: WorkIdentity) extends WorkUnitEvent {

    override def id: Long = 1

    override def stop(): Unit = {
      print(s"STOP $this\n")
    }
  }
}
