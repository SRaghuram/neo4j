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
  class VerifyingQueryExecutionTracer(queryId: Int) extends QueryExecutionTracer {
    private var workUnitId = -1
    override def scheduleWorkUnit(task: Task[_ <: AutoCloseable],
                                  upstreamWorkUnitEvent: Option[WorkUnitEvent]): ScheduledWorkUnitEvent = {
      workUnitId += 1
      val upstreamId = upstreamWorkUnitEvent.map(_.asInstanceOf[VerifyingWorkUnitEvent].workUnitId)
      verifier(TASK_SCHEDULE(queryId, task, workUnitId, upstreamId))
      new VerifyingScheduledWorkUnitEvent(queryId, task, workUnitId)
    }

    override def stopQuery(): Unit = {
      verifier(QUERY_STOP(queryId))
    }
  }

  class VerifyingScheduledWorkUnitEvent(queryId: Int, task: Task[_ <: AutoCloseable], workUnitId: Int) extends ScheduledWorkUnitEvent {
    override def start(): WorkUnitEvent = {
      verifier(TASK_START(queryId, task, workUnitId))
      new VerifyingWorkUnitEvent(queryId, task, workUnitId)
    }
  }

  class VerifyingWorkUnitEvent(queryId: Int, task: Task[_ <: AutoCloseable], val workUnitId: Int) extends WorkUnitEvent {
    override def id: Long = 0

    override def stop(): Unit = {
      verifier(TASK_STOP(queryId, task, workUnitId))
    }
  }
}




sealed trait Event
case class QUERY_START(queryId: Int) extends Event
case class QUERY_STOP(queryId: Int) extends Event
case class TASK_SCHEDULE(queryId: Int, task: Task[_ <: AutoCloseable], workUnitId: Int, upstream: Option[Int]) extends Event
case class TASK_START(queryId: Int, task: Task[_ <: AutoCloseable], workUnitId: Int) extends Event
case class TASK_STOP(queryId: Int, task: Task[_ <: AutoCloseable], workUnitId: Int) extends Event

trait TracerVerifier {

  def apply(e: Event): Unit
}
