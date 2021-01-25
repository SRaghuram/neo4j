/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.ScheduledWorkUnitEvent
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

class ComposingSchedulerTracer(val inners: SchedulerTracer*) extends SchedulerTracer {
  override def traceQuery(): QueryExecutionTracer = {
    new ComposingQueryExecutionTracer(inners.map(_.traceQuery()))
  }
}

class ComposingQueryExecutionTracer(inners: Seq[QueryExecutionTracer]) extends QueryExecutionTracer {
  override def scheduleWorkUnit(workId: WorkIdentity, upstreamWorkUnitEvent: WorkUnitEvent): ScheduledWorkUnitEvent = {
    new ComposingScheduledWorkUnitEvent(inners.map(_.scheduleWorkUnit(workId, upstreamWorkUnitEvent)))
  }

  override def stopQuery(): Unit = inners.foreach(_.stopQuery())
}

class ComposingScheduledWorkUnitEvent(inners: Seq[ScheduledWorkUnitEvent]) extends ScheduledWorkUnitEvent {
  override def start(): WorkUnitEvent = new ComposingWorkUnitEvent(inners.map(_.start()))
}

class ComposingWorkUnitEvent(inners: Seq[WorkUnitEvent]) extends WorkUnitEvent {
  override def id: Long = inners.head.id

  override def stop(): Unit = inners.foreach(_.stop())
}
