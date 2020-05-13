/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.ScheduledWorkUnitEvent
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

class ParallelismTracer extends SchedulerTracer {

  private val _workerCount = new AtomicInteger()
  private val x: ThreadLocal[Int] = ThreadLocal.withInitial(() => _workerCount.getAndIncrement())

  private val workUnitEventLists = new ConcurrentLinkedQueue[ArrayBuffer[TestWorkUnitEvent]]()
  private val workUnitEvents: ThreadLocal[ArrayBuffer[TestWorkUnitEvent]] =
    ThreadLocal.withInitial(
      () => {
        val x = new ArrayBuffer[TestWorkUnitEvent]()
        workUnitEventLists.add(x)
        x
      }
    )

  def hasEvidenceOfParallelism: Boolean = {

    val allWorkUnitEvents: Map[Id, immutable.IndexedSeq[TestWorkUnitEvent]] =
      workUnitEventLists.asScala.flatten.toIndexedSeq.sortBy(_.startTime).groupBy(_.workId)

    for (workIdGroup <- allWorkUnitEvents.values) {
      var previousStop = workIdGroup.head.stopTime
      for (workUnitEvent <- workIdGroup.tail) {
        if (workUnitEvent.startTime < previousStop)
          return true
        else
          previousStop = workUnitEvent.stopTime
      }
    }

    false
  }

  override def traceQuery(): QueryExecutionTracer = new TestQueryExecutionTracer

  private class TestQueryExecutionTracer extends QueryExecutionTracer {

    override def scheduleWorkUnit(workId: WorkIdentity,
                                  upstreamWorkUnitEvent: WorkUnitEvent): ScheduledWorkUnitEvent =
      new TestScheduledWorkUnitEvent(workId.workId, workId.workDescription)

    override def stopQuery(): Unit = {}
  }

  private class TestScheduledWorkUnitEvent(workId: Id, workDescription: String) extends ScheduledWorkUnitEvent {

    override def start(): WorkUnitEvent = {
      x.get()

      new TestWorkUnitEvent(System.nanoTime(), workId, workDescription)
    }
  }

  private class TestWorkUnitEvent(val startTime: Long,
                                  val workId: Id,
                                  val workDescription: String) extends WorkUnitEvent {

    var stopTime: Long = -1L

    override def id: Long = 1

    override def stop(): Unit = {
      stopTime = System.nanoTime()
      workUnitEvents.get() += this
    }
  }
}
