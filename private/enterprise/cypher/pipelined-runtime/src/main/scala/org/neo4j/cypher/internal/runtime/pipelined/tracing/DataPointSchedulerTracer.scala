/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

/**
 * Scheduler tracer that collect times and thread ids of events and report them as DataPoints.
 */
class DataPointSchedulerTracer(dataPointWriter: DataPointWriter) extends SchedulerTracer {

  private val queryCounter = new AtomicInteger()
  private val localWorkUnitId = ThreadLocal.withInitial(new Supplier[Long] {override def get(): Long = 0})

  private def newWorkUnitId(schedulingThreadId: Long): Long = {
    val localId = localWorkUnitId.get()
    localWorkUnitId.set(localId + 1)
    localId | (schedulingThreadId << 40)
  }

  override def traceQuery(): QueryExecutionTracer =
    QueryTracer(queryCounter.incrementAndGet())

  case class QueryTracer(queryId: Int) extends QueryExecutionTracer {
    override def scheduleWorkUnit(workId: WorkIdentity, upstreamWorkUnit: WorkUnitEvent): ScheduledWorkUnitEvent = {
      val scheduledTime = currentTime()
      val schedulingThread = Thread.currentThread().getId
      val workUnitId = newWorkUnitId(schedulingThread)
      val maybeUpstreamWorkUnitId = Option(upstreamWorkUnit).map(_.id)
      ScheduledWorkUnit(workUnitId, maybeUpstreamWorkUnitId, queryId, scheduledTime, schedulingThread, workId)
    }

    override def stopQuery(): Unit = {}
  }

  case class ScheduledWorkUnit(workUnitId: Long,
                               upstreamWorkUnitId: Option[Long],
                               queryId: Int,
                               scheduledTime: Long,
                               schedulingThreadId: Long,
                               workId: WorkIdentity) extends ScheduledWorkUnitEvent {

    override def start(): WorkUnitEvent = {
      val startTime = currentTime()
      WorkUnit(workUnitId,
        upstreamWorkUnitId,
        queryId,
        schedulingThreadId,
        scheduledTime,
        Thread.currentThread().getId,
        startTime,
        workId)
    }
  }

  case class WorkUnit(override val id: Long,
                      upstreamId: Option[Long],
                      queryId: Int,
                      schedulingThreadId: Long,
                      scheduledTime: Long,
                      executionThreadId: Long,
                      startTime: Long,
                      workId: WorkIdentity) extends WorkUnitEvent {

    override def stop(): Unit = {
      val stopTime = currentTime()
      dataPointWriter.write(
        DataPoint(id, upstreamId, queryId, schedulingThreadId, scheduledTime, executionThreadId, startTime, stopTime, workId))
    }
  }

  private def currentTime(): Long = System.nanoTime()
}
