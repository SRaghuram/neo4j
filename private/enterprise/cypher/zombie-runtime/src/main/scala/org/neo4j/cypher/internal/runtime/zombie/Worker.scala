/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

import org.neo4j.cypher.internal.runtime.morsel.QueryResources
import org.neo4j.cypher.internal.runtime.scheduling.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.zombie.execution.{ExecutingQuery, QueryManager, SchedulingPolicy}

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
             sleeper: Sleeper = new Sleeper) extends Runnable {

  override def run(): Unit = {
    while (!Thread.interrupted()) {
      try {
        val executingQuery = queryManager.nextQueryToWorkOn(workerId)
        if (executingQuery != null) {
          val worked = workOnQuery(executingQuery)
          if (!worked) {
            sleeper.reportIdle()
          } else {
            sleeper.reset()
          }
        } else {
          sleeper.reportIdle()
        }
      } catch {
        // Failure in QueryManager. Crash horribly.
        case error: Throwable =>
          error.printStackTrace()
          throw error
      }
    }
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
          state.putContinuation(task)
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

class Sleeper(private val idleThreshold: Int = 10000,
              private val sleepDuration: Duration = Duration(1, TimeUnit.SECONDS)) {
  private val sleepNs = sleepDuration.toNanos
  private var idleCounter = 0

  def reset(): Unit = {
    idleCounter = 0
  }

  def reportIdle(): Unit = {
    idleCounter += 1
    if (idleCounter > idleThreshold) {
      LockSupport.parkNanos(sleepNs)
    }
  }
}
