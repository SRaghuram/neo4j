/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.scheduling

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

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
    override def scheduleWorkUnit(task: Task[_ <: AutoCloseable], upstreamWorkUnits: Seq[WorkUnitEvent]): ScheduledWorkUnitEvent = {
      val scheduledTime = currentTime()
      val schedulingThread = Thread.currentThread().getId
      val workUnitId = newWorkUnitId(schedulingThread)
      val upstreamWorkUnitIds = upstreamWorkUnits.map(_.id)
      ScheduledWorkUnit(workUnitId, upstreamWorkUnitIds, queryId, scheduledTime, schedulingThread, task)
    }

    override def stopQuery(): Unit = {}
  }

  case class ScheduledWorkUnit(workUnitId: Long,
                               upstreamWorkUnitIds: Seq[Long],
                               queryId: Int,
                               scheduledTime: Long,
                               schedulingThreadId: Long,
                               task: Task[_ <: AutoCloseable]) extends ScheduledWorkUnitEvent {

    override def start(): WorkUnitEvent = {
      val startTime = currentTime()
      WorkUnit(workUnitId,
        upstreamWorkUnitIds,
               queryId,
               schedulingThreadId,
               scheduledTime,
               Thread.currentThread().getId,
               startTime,
               task)
    }
  }

  case class WorkUnit(override val id: Long,
                      upstreamIds: Seq[Long],
                      queryId: Int,
                      schedulingThreadId: Long,
                      scheduledTime: Long,
                      executionThreadId: Long,
                      startTime: Long,
                      task: Task[_ <: AutoCloseable]) extends WorkUnitEvent {

    override def stop(): Unit = {
      val stopTime = currentTime()
      dataPointWriter.write(
        DataPoint(id, upstreamIds, queryId, schedulingThreadId, scheduledTime, executionThreadId, startTime, stopTime, task))
    }
  }

  private def currentTime(): Long = System.nanoTime()
}
