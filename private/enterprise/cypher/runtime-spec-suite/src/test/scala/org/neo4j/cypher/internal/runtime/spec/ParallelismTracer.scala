/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

import org.neo4j.cypher.internal.runtime.scheduling._

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

class ParallelismTracer extends SchedulerTracer {

  private val _workerCount = new AtomicInteger()
  private val x: ThreadLocal[Int] = ThreadLocal.withInitial(new Supplier[Int] {override def get(): Int = _workerCount.getAndIncrement()})

  private val workUnitEventLists = new ConcurrentLinkedQueue[ArrayBuffer[TestWorkUnitEvent]]()
  private val workUnitEvents: ThreadLocal[ArrayBuffer[TestWorkUnitEvent]] =
    ThreadLocal.withInitial(
      new Supplier[ArrayBuffer[TestWorkUnitEvent]] {
        override def get(): ArrayBuffer[TestWorkUnitEvent] = {
          val x = new ArrayBuffer[TestWorkUnitEvent]()
          workUnitEventLists.add(x)
          x
        }
      }
    )

  def hasEvidenceOfParallelism: Boolean = {
    import scala.collection.JavaConverters._

    val allWorkUnitEvents: Map[Int, immutable.IndexedSeq[TestWorkUnitEvent]] =
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

    override def scheduleWorkUnit(task: Task[_ <: AutoCloseable],
                                  upstreamWorkUnitEvent: Option[WorkUnitEvent]): ScheduledWorkUnitEvent =
      new TestScheduledWorkUnitEvent(task.workId, task.workDescription)

    override def stopQuery(): Unit = {}
  }

  private class TestScheduledWorkUnitEvent(workId: Int, workDescription: String) extends ScheduledWorkUnitEvent {

    override def start(): WorkUnitEvent = {
      x.get()

      new TestWorkUnitEvent(System.nanoTime(), workId, workDescription)
    }
  }

  private class TestWorkUnitEvent(val startTime: Long,
                          val workId: Int,
                          val workDescription: String) extends WorkUnitEvent {

    var stopTime: Long = -1L

    override def id: Long = 1

    override def stop(): Unit = {
      stopTime = System.nanoTime()
      workUnitEvents.get() += this
    }
  }
}

