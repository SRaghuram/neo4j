/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.scheduling

class VerifyingSchedulerTracer(verifier: TracerVerifier) extends SchedulerTracer {
  private var queryId = -1
  override def traceQuery(): QueryExecutionTracer = {
    queryId += 1
    verifier(QUERY_START(queryId))
    new VerifyingQueryExecutionTracer(queryId)
  }
  class VerifyingQueryExecutionTracer(queryId: Long) extends QueryExecutionTracer {
    private var workUnitId = -1
    override def scheduleWorkUnit(task: Task[_ <: AutoCloseable],
                                  upstreamWorkUnitEvents: Seq[WorkUnitEvent]): ScheduledWorkUnitEvent = {
      workUnitId += 1
      val upstreamIds = upstreamWorkUnitEvents.map(_.id)
      verifier(TASK_SCHEDULE(queryId, task, workUnitId, upstreamIds))
      new VerifyingScheduledWorkUnitEvent(queryId, task, workUnitId)
    }

    override def stopQuery(): Unit = {
      verifier(QUERY_STOP(queryId))
    }
  }

  class VerifyingScheduledWorkUnitEvent(queryId: Long, task: Task[_ <: AutoCloseable], workUnitId: Long) extends ScheduledWorkUnitEvent {
    override def start(): WorkUnitEvent = {
      verifier(TASK_START(queryId, task, workUnitId))
      new VerifyingWorkUnitEvent(queryId, task, workUnitId)
    }
  }

  class VerifyingWorkUnitEvent(queryId: Long, task: Task[_ <: AutoCloseable], workUnitId: Long) extends WorkUnitEvent {
    override def id: Long = workUnitId

    override def stop(): Unit = {
      verifier(TASK_STOP(queryId, task, workUnitId))
    }
  }
}




sealed trait Event
case class QUERY_START(queryId: Long) extends Event
case class QUERY_STOP(queryId: Long) extends Event
case class TASK_SCHEDULE(queryId: Long, task: Task[_ <: AutoCloseable], workUnitId: Long, upstreams: Seq[Long]) extends Event
case class TASK_START(queryId: Long, task: Task[_ <: AutoCloseable], workUnitId: Long) extends Event
case class TASK_STOP(queryId: Long, task: Task[_ <: AutoCloseable], workUnitId: Long) extends Event

trait TracerVerifier {

  def apply(e: Event): Unit
}
