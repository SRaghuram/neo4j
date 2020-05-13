/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

/**
  * Scheduler tracer. Globally traces query execution..
  */
trait SchedulerTracer {
  /**
    * Trace a query execution
    */
  def traceQuery(): QueryExecutionTracer
}

/**
  * Tracer for a particular query execution
  */
trait QueryExecutionTracer {

  /**
    * Trace the scheduling of a work unit for this query execution
    */
  def scheduleWorkUnit(workId: WorkIdentity, upstreamWorkUnitEvent: WorkUnitEvent): ScheduledWorkUnitEvent

  /**
    * End of query execution
    */
  def stopQuery(): Unit
}

/**
  * Work unit event of a particular query execution
  */
trait ScheduledWorkUnitEvent {

  /**
    * Trace the start of a work unit event for this query execution
    */
  def start(): WorkUnitEvent
}

/**
  * Work unit event of a particular query execution
  */
trait WorkUnitEvent {

  /**
    * Unique identifier
    */
  def id: Long

  /**
    * Trace the stop of this work unit event.
    */
  def stop(): Unit
}

object SchedulerTracer {
  val NoSchedulerTracer: SchedulerTracer = () => NoQueryExecutionTracer

  val NoQueryExecutionTracer: QueryExecutionTracer = new QueryExecutionTracer {
    override def scheduleWorkUnit(workId: WorkIdentity, upstreamWorkUnitEvent: WorkUnitEvent): ScheduledWorkUnitEvent = NoScheduledWorkUnitEvent
    override def stopQuery(): Unit = {}
  }

  val NoScheduledWorkUnitEvent: ScheduledWorkUnitEvent = new ScheduledWorkUnitEvent {
    override def start(): WorkUnitEvent = NoWorkUnitEvent
  }

  val NoWorkUnitEvent: WorkUnitEvent = new WorkUnitEvent {
    override def stop(): Unit = {}

    override def id: Long = -1
  }
}

