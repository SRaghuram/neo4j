/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

class VerifyingSchedulerTracer(verifier: TracerVerifier) extends SchedulerTracer {
  private var queryId = -1
  override def traceQuery(): QueryExecutionTracer = {
    queryId += 1
    verifier(QUERY_START(queryId))
    new VerifyingQueryExecutionTracer(queryId)
  }
  class VerifyingQueryExecutionTracer(queryId: Long) extends QueryExecutionTracer {
    private var workUnitId = -1
    override def scheduleWorkUnit(workId: WorkIdentity, upstreamWorkUnitEvent: WorkUnitEvent): ScheduledWorkUnitEvent = {
      workUnitId += 1
      val upstreamIds = Option(upstreamWorkUnitEvent).map(_.id).toList
      verifier(TASK_SCHEDULE(queryId, workId, workUnitId, upstreamIds))
      new VerifyingScheduledWorkUnitEvent(queryId, workId, workUnitId)
    }

    override def stopQuery(): Unit = {
      verifier(QUERY_STOP(queryId))
    }
  }

  class VerifyingScheduledWorkUnitEvent(queryId: Long, workId: WorkIdentity, workUnitId: Long) extends ScheduledWorkUnitEvent {
    override def start(): WorkUnitEvent = {
      verifier(TASK_START(queryId, workId, workUnitId))
      new VerifyingWorkUnitEvent(queryId, workId, workUnitId)
    }
  }

  class VerifyingWorkUnitEvent(queryId: Long, workId: WorkIdentity, workUnitId: Long) extends WorkUnitEvent {
    override def id: Long = workUnitId

    override def stop(): Unit = {
      verifier(TASK_STOP(queryId, workId, workUnitId))
    }
  }
}

sealed trait Event
case class QUERY_START(queryId: Long) extends Event
case class QUERY_STOP(queryId: Long) extends Event
case class TASK_SCHEDULE(queryId: Long, workId: WorkIdentity, workUnitId: Long, upstreams: Seq[Long]) extends Event
case class TASK_START(queryId: Long, workId: WorkIdentity, workUnitId: Long) extends Event
case class TASK_STOP(queryId: Long, workId: WorkIdentity, workUnitId: Long) extends Event

trait TracerVerifier {

  def apply(e: Event): Unit
}
