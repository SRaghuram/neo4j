/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import java.util.concurrent.{ThreadFactory, TimeUnit}
import java.util.concurrent.locks.LockSupport

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.concurrent.duration.Duration

class WorkerManager(val numberOfWorkers: Int, threadFactory: ThreadFactory) extends WorkerWaker with Lifecycle {
  val queryManager = new QueryManager

  protected val _workers: Array[Worker] =
    (for (workerId <- 0 until numberOfWorkers) yield {
      new Worker(workerId, queryManager, LazyScheduling,  Sleeper.concurrentSleeper(workerId))
    }).toArray

  def workers: Seq[Worker] = _workers

  def assertNoWorkerIsActive(): Unit = {
    val activeWorkers =
      for {
        worker <- _workers.filter(_.sleeper.isWorking)
      } yield Worker.WORKING_THOUGH_RELEASED(worker)

    if (activeWorkers.nonEmpty) {
      throw new RuntimeResourceLeakException(activeWorkers.mkString("\n"))
    }
  }

  // ========== WORKER WAKER ===========

  override def wakeOne(): Unit = {
    var i = 0
    while (i < _workers.length) {
      if (_workers(i).isSleeping) {
        LockSupport.unpark(workerThreads(i))
        return
      }
      i += 1
    }
  }

  // ========== LIFECYCLE ===========

  private val threadJoinWait = Duration(1, TimeUnit.MINUTES)

  @volatile private var workerThreads: Array[Thread] = _

  override def init(): Unit = {}

  override def start(): Unit = {
    DebugLog.log("starting worker threads")
    _workers.foreach(_.reset())
    workerThreads = _workers.map(threadFactory.newThread(_))
    workerThreads.foreach(_.start())
    DebugLog.logDiff("done")
  }

  override def stop(): Unit = {
    DebugLog.log("stopping worker threads")
    _workers.foreach(_.stop())
    workerThreads.foreach(LockSupport.unpark)
    workerThreads.foreach(_.join(threadJoinWait.toMillis))
    DebugLog.logDiff("done")
  }

  override def shutdown(): Unit = {}
}
