/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.debug.{DebugLog, DebugSupport}
import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner

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
             val sleeper: Sleeper,
             val resources: QueryResources) extends Runnable {

  @volatile
  var isTimeToStop = true

  def isSleeping: Boolean = sleeper.isSleeping

  override def run(): Unit = {
    DebugSupport.logWorker(s"Worker($workerId) started")
    while (!isTimeToStop) {
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
    var workUnitEvent: WorkUnitEvent = null
    try {
      executingQuery.bindTransactionToThread()

      DebugLog.log("[WORKER%2d] working on %s".format(workerId, task))

      sleeper.reportStartWorkUnit()
      workUnitEvent = executingQuery.queryExecutionTracer.scheduleWorkUnit(task, upstreamWorkUnitEvents(task)).start()
      task
        .executeWorkUnit(resources, workUnitEvent, executingQuery.workersQueryProfiler.queryProfiler(workerId))
        .produce()

    } finally {
      if (workUnitEvent != null) {
        workUnitEvent.stop()
        sleeper.reportStopWorkUnit()
      }
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
    AssertionRunner.runUnderAssertion { () =>
      val liveCounts = new LiveCounts()
      collectCursorLiveCounts(liveCounts)
      liveCounts.assertAllReleased()

      if (sleeper.isWorking) {
        throw new RuntimeResourceLeakException(Worker.WORKING_THOUGH_RELEASED(this))
      }
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
