/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryManager
import org.neo4j.cypher.internal.runtime.pipelined.execution.WorkerWaker
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.concurrent.duration.Duration

/**
 * Management of Workers
 */
trait WorkerManagement extends WorkerWaker {
  /**
   * @return the query manager
   */
  def queryManager: QueryManager

  /**
   * @return the workers
   */
  def workers: Seq[Worker]

  /**
   * @return the number of workers
   */
  def numberOfWorkers: Int

  /**
   * Throw an exception if any worker is active.
   */
  def assertNoWorkerIsActive(): Boolean

  /**
   * @return `true` if there is at least one Worker for this WorkerManager. `false` otherwise.
   */
  def hasWorkers: Boolean

  /**
   * Wait until all workers have completed ongoing work according to current demand and settled down in an idle state
   *
   * @param timeoutMs Timeout in ms
   * @return true if all workers settled in an idle state, or false if the timeout occurred
   */
  def waitForWorkersToIdle(timeoutMs: Int): Boolean
}

class WorkerManager(val numberOfWorkers: Int, threadFactory: ThreadFactory) extends WorkerManagement with Lifecycle {
  override val queryManager = new QueryManager

  private val _workers: Array[Worker] =
    (for (workerId <- 0 until numberOfWorkers) yield {
      new Worker(workerId, queryManager, Sleeper.concurrentSleeper(workerId))
    }).toArray

  override def workers: Seq[Worker] = _workers

  override def assertNoWorkerIsActive(): Boolean = {
    val activeWorkers =
      for {
        worker <- _workers.filter(_.sleeper.isWorking)
      } yield Worker.WORKING_THOUGH_RELEASED(worker)

    if (activeWorkers.nonEmpty) {
      throw new RuntimeResourceLeakException(activeWorkers.mkString("\n"))
    }
    true
  }

  override def waitForWorkersToIdle(timeoutMs: Int): Boolean = {
    val startTime = System.nanoTime()
    val stopTime = startTime + timeoutMs.toLong * 1000000L
    var currentTime = startTime
    while (currentTime < stopTime) {
      if (!_workers.exists(_.sleeper.isWorking)) {
        return true
      }
      LockSupport.parkNanos(1000000L)
      currentTime = System.nanoTime()
    }
    !_workers.exists(_.sleeper.isWorking)
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

  override def hasWorkers: Boolean = {
    _workers.nonEmpty
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
