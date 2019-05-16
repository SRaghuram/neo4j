/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.{ExecutingQuery, QueryManager, QueryResources, SchedulingPolicy}
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent

import scala.concurrent.duration.Duration

/**
  * Worker which executes query work, one task at a time. Asks [[QueryManager]] for the
  * next [[ExecutingQuery]] to work on. Then asks [[SchedulingPolicy]] for a suitable
  * task to perform on that query.
  *
  * A worker has it's own [[QueryResources]] which it will use to execute tasks.
  *
  * TODO: integrate with lifecycle to ensure these are closed cleanly
  */
class Worker(val workerId: Int,
             queryManager: QueryManager,
             schedulingPolicy: SchedulingPolicy,
             resources: QueryResources,
             var sleeper: Sleeper = null) extends Runnable {

  if (sleeper == null)
    sleeper = new Sleeper(workerId)

  def isSleepy: Boolean = sleeper.isSleepy

  override def run(): Unit = {
    DebugSupport.logWorker(s"Worker($workerId) started")
    while (!Thread.interrupted()) {
      try {
        val executingQuery = queryManager.nextQueryToWorkOn(workerId)
        if (executingQuery != null) {
          val worked = workOnQuery(executingQuery)
          if (!worked) {
            sleeper.reportIdle()
          } else {
            sleeper.reportWork()
          }
        } else {
          sleeper.reportIdle()
        }
      } catch {
        // Failure in QueryManager. Crash horribly.
        case error: Throwable =>
          DebugSupport.logWorker(s"Worker($workerId) crashed horribly")
          error.printStackTrace()
          throw error
      }
    }
    DebugSupport.logWorker(s"Worker($workerId) stopped")
  }

  /**
    * Try to obtain a task for a given query and work on it.
    *
    * @param executingQuery the query
    * @return if some work was performed
    */
  def workOnQuery(executingQuery: ExecutingQuery): Boolean = {
    try {
      val task = schedulingPolicy.nextTask(executingQuery, resources)
      if (task != null) {
        val state = task.pipelineState

        try {
          executingQuery.bindTransactionToThread()

          val workUnitEvent = executingQuery.queryExecutionTracer.scheduleWorkUnit(task, upstreamWorkUnitEvents(task)).start()
          val preparedOutput = task.executeWorkUnit(resources, workUnitEvent)
          workUnitEvent.stop()
          preparedOutput.produce()
        } finally {
          executingQuery.unbindTransaction()
        }

        if (task.canContinue) {
          // Put the continuation before unlocking (closeWorkUnit)
          // so that in serial pipelines we can guarantee that the continuation
          // is the next thing which is picked up
          state.putContinuation(task, false)
          state.closeWorkUnit()
        } else {
          task.close()
        }
        true
      } else {
        false
      }
    } catch {

      // Failure in scheduling or execution of the query
      case throwable: Throwable =>
        executingQuery.executionState.failQuery(throwable)
        false
    }
  }

  private def upstreamWorkUnitEvents(task: PipelineTask): Seq[WorkUnitEvent] = {
    val upstreamWorkUnitEvent = task.startTask.producingWorkUnitEvent
    if (upstreamWorkUnitEvent != null) Seq(upstreamWorkUnitEvent) else Seq.empty
  }
}

class Sleeper(val workerId: Int,
              private val sleepDuration: Duration = Duration(1, TimeUnit.SECONDS)) {

  private val sleepNs = sleepDuration.toNanos
  private var isIdle = false
  private var workStreak = 0
  @volatile private var sleepy: Boolean = false

  def reportWork(): Unit = {
    workStreak += 1
  }

  def reportIdle(): Unit = {
    DebugSupport.logWorker(s"Worker($workerId) parked after working $workStreak times")
    workStreak = 0
    sleepy = true
    LockSupport.parkNanos(sleepNs)
    DebugSupport.logWorker(s"Worker($workerId) unparked")
    sleepy = false
  }

  def isSleepy: Boolean = sleepy
}
