/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.debug.{DebugLog, DebugSupport}
import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent

import scala.concurrent.duration.Duration

/**
  * Worker which executes query work, one task at a time. Asks [[QueryManager]] for the
  * next [[ExecutingQuery]] to work on. Then asks [[SchedulingPolicy]] for a suitable
  * task to perform on that query.
  *
  * A worker has it's own [[QueryResources]] which it will use to execute tasks.
  */
class Worker(val workerId: Int,
             queryManager: QueryManager,
             schedulingPolicy: SchedulingPolicy,
             val resources: QueryResources,
             var sleeper: Sleeper = null) extends Runnable {

  if (sleeper == null)
    sleeper = new Sleeper(workerId)

  def isSleeping: Boolean = sleeper.isSleeping

  override def run(): Unit = {
    DebugSupport.logWorker(s"Worker($workerId) started")
    while (!Thread.interrupted()) {
      try {
        val executingQuery = queryManager.nextQueryToWorkOn(workerId)
        if (executingQuery != null) {
          val worked = workOnQuery(executingQuery)
          if (!worked) {
            sleeper.reportIdle()
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
    * @return `true` if some work was performed, otherwise `false`
    */
  def workOnQuery(executingQuery: ExecutingQuery): Boolean = {
    val task = scheduleNextTask(executingQuery)
    if (task == null) {
      false
    } else {
      try {
        val state = task.pipelineState

        executeTask(executingQuery, task)

        if (task.canContinue) {
          // Put the continuation before unlocking (closeWorkUnit)
          // so that in serial pipelines we can guarantee that the continuation
          // is the next thing which is picked up
          state.putContinuation(task, wakeUp = false, resources)
        } else {
          task.close(resources)
        }
        true
      } catch {
        // Failure while executing `task`
        case throwable: Throwable =>
          executingQuery.executionState.failQuery(throwable, resources, task.pipelineState.pipeline)
          task.close(resources)
          true
      }
    }
  }

  private def executeTask(executingQuery: ExecutingQuery,
                          task: PipelineTask): Unit = {
    try {
      executingQuery.bindTransactionToThread()

      DebugLog.log("[WORKER%2d] working on %s".format(workerId, task))

      sleeper.reportStartWorkUnit()
      val workUnitEvent = executingQuery.queryExecutionTracer.scheduleWorkUnit(task,
                                                                               upstreamWorkUnitEvents(task)).start()
      val preparedOutput =
        try {
          task.executeWorkUnit(resources, workUnitEvent, executingQuery.workersQueryProfiler.queryProfiler(workerId))
        } finally {
          workUnitEvent.stop()
          sleeper.reportStopWorkUnit()
        }
      preparedOutput.produce()
    } finally {
      executingQuery.unbindTransaction()
    }
  }

  private def scheduleNextTask(executingQuery: ExecutingQuery): PipelineTask = {
    try {
      schedulingPolicy.nextTask(executingQuery, resources)
    } catch {
      // Failure in nextTask of a pipeline, after taking Morsel
      case NextTaskException(pipeline, SchedulingInputException(morsel: MorselParallelizer, cause)) =>
        executingQuery.executionState.closeMorselTask(pipeline, morsel.nextCopy)
        executingQuery.executionState.failQuery(cause, resources, pipeline)
        null

      // Failure in nextTask of a pipeline, after taking Accumulator
      case NextTaskException(pipeline, SchedulingInputException(acc: MorselAccumulator[_], cause)) =>
        executingQuery.executionState.closeAccumulatorTask(pipeline, acc)
        executingQuery.executionState.failQuery(cause, resources, pipeline)
        null

      // Failure in nextTask of a pipeline
      case NextTaskException(pipeline, cause) =>
        executingQuery.executionState.failQuery(cause, resources, pipeline)
        null

      // Failure in scheduling query
      case throwable: Throwable =>
        executingQuery.executionState.failQuery(throwable, resources, null)
        null
    }
  }

  def close(): Unit = {
    resources.close()
  }

  def collectCursorLiveCounts(acc: LiveCounts): Unit = {
    resources.cursorPools.collectLiveCounts(acc)
  }

  def assertAllReleased(): Unit = {
    val liveCounts = new LiveCounts()
    collectCursorLiveCounts(liveCounts)
    liveCounts.assertAllReleased()

    if (sleeper.isWorking) {
      throw new RuntimeResourceLeakException(Worker.WORKING_THOUGH_RELEASED(this))
    }
  }

  private def upstreamWorkUnitEvents(task: PipelineTask): Seq[WorkUnitEvent] = {
    val upstreamWorkUnitEvent = task.startTask.producingWorkUnitEvent
    if (upstreamWorkUnitEvent != null) Array(upstreamWorkUnitEvent) else Worker.NO_WORK
  }

  override def toString: String = s"Worker[$workerId, ${sleeper.statusString}]"
}

object Worker {
  val NO_WORK: Seq[WorkUnitEvent] = Array.empty[WorkUnitEvent]

  def WORKING_THOUGH_RELEASED(worker: Worker): String =
    s"$worker is WORKING even though all resources should be released!"
}

object Sleeper {
  sealed trait Status
  case object ACTIVE extends Status // while the worker is not doing any of the below, e.g. looking for work
  case object WORKING extends Status // while the worker is executing a work unit of some query
  case object SLEEPING extends Status // while the worker is parked and not looking for work
}

class Sleeper(val workerId: Int,
              private val sleepDuration: Duration = Duration(1, TimeUnit.SECONDS)) {

  import Sleeper._

  private val sleepNs = sleepDuration.toNanos
  private var workStreak = 0
  @volatile private var status: Status = ACTIVE

  def reportStartWorkUnit(): Unit = {
    status = WORKING
  }

  def reportStopWorkUnit(): Unit = {
    status = ACTIVE
    workStreak += 1
  }

  def reportIdle(): Unit = {
    DebugSupport.logWorker(s"Worker($workerId) parked after working $workStreak times")
    workStreak = 0
    status = SLEEPING
    LockSupport.parkNanos(sleepNs)
    DebugSupport.logWorker(s"Worker($workerId) unparked")
    status = ACTIVE
  }

  def isSleeping: Boolean = status == SLEEPING
  def isWorking: Boolean = status == WORKING
  def statusString: String = status.toString
}
