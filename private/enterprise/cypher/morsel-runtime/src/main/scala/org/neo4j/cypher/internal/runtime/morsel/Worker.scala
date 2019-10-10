/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.{NonFatalCypherError, RuntimeResourceLeakException}
import org.neo4j.cypher.internal.runtime.debug.{DebugLog, DebugSupport}
import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.cypher.internal.runtime.morsel.operators.PreparedOutput
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent

/**
  * Worker which executes query work, one task at a time. Asks [[QueryManager]] for the
  * next [[ExecutingQuery]] to work on. Then asks [[SchedulingPolicy]] for a suitable
  * task to perform on that query.
  *
  * A worker has it's own [[QueryResources]] which it will use to execute tasks.
  */
class Worker(val workerId: Int,
             queryManager: QueryManager,
             val schedulingPolicy: SchedulingPolicy,
             val sleeper: Sleeper) extends Runnable {

  @volatile
  private var isTimeToStop = false

  def reset(): Unit = {
    isTimeToStop = false
  }

  def stop(): Unit = {
    isTimeToStop = true
  }

  def isSleeping: Boolean = sleeper.isSleeping

  override def run(): Unit = {
    DebugSupport.WORKERS.log("[WORKER%2d] started", workerId)
    while (!isTimeToStop) {
      try {
        val executingQuery = queryManager.nextQueryToWorkOn(workerId)
        if (executingQuery != null) {
          val resources = executingQuery.workerResourceProvider.resourcesForWorker(workerId)
          val worked = workOnQuery(executingQuery, resources)
          if (!worked) {
            sleeper.reportIdle()
          }
        } else {
          sleeper.reportIdle()
        }
      } catch {
        // Failure in QueryManager. Crash horribly.
        case error: Throwable =>
          DebugSupport.WORKERS.log("[WORKER%2d] crashed horribly", workerId)
          error.printStackTrace()
          throw error
      }
    }
    DebugSupport.WORKERS.log("[WORKER%2d] stopped", workerId)
  }

  /**
    * Try to obtain a task for a given query and work on it.
    *
    * @param executingQuery the query
    * @param resources      the query resources for this worker
    * @return `true` if some work was performed or cancelled, otherwise `false`
    */
  def workOnQuery(executingQuery: ExecutingQuery, resources: QueryResources): Boolean = {
    val schedulingResult = scheduleNextTask(executingQuery, resources)
    if (schedulingResult.task == null) {
      schedulingResult.someTaskWasFilteredOut
    } else {
      try {
        schedulingResult.task match {
          case cleanUpTask: CleanUpTask =>
            cleanUpTask.executeWorkUnit(resources, null, null)
          case pipelineTask: PipelineTask =>
            val state = pipelineTask.pipelineState

            executeTask(executingQuery, pipelineTask, resources)

            if (pipelineTask.canContinue) {
              state.putContinuation(pipelineTask, wakeUp = false, resources)
            } else {
              pipelineTask.close(resources)
            }
        }
        true
      } catch {
        // Failure while executing `task`
        case NonFatalCypherError(throwable) =>
          try {
            schedulingResult.task match {
              case pipelineTask: PipelineTask =>
                executingQuery.executionState.failQuery(throwable, resources, pipelineTask.pipelineState.pipeline)
                pipelineTask.close(resources)
            }
          } catch {
            case NonFatalCypherError(t2) =>
              // Cleaning up also failed
              if (throwable != t2) {
                throwable.addSuppressed(t2)
              }
              // We would actually want a hard shutdown here
              throwable.printStackTrace()
          }
          true
      }
    }
  }

  protected[morsel] def executeTask(executingQuery: ExecutingQuery,
                          task: PipelineTask,
                          resources: QueryResources): Unit = {
    var workUnitEvent: WorkUnitEvent = null
    var preparedOutput: PreparedOutput = null
    try {

      DebugLog.log("[WORKER%2d] working on %s", workerId, task)
      DebugSupport.WORKERS.log("[WORKER%2d] working on %s of %s", workerId, task, executingQuery)

      sleeper.reportStartWorkUnit()
      workUnitEvent = executingQuery.queryExecutionTracer.scheduleWorkUnit(task, upstreamWorkUnitEvents(task)).start()
      preparedOutput = task.executeWorkUnit(resources, workUnitEvent, executingQuery.workersQueryProfiler.queryProfiler(workerId))
    } finally {
      if (workUnitEvent != null) {
        workUnitEvent.stop()
        sleeper.reportStopWorkUnit()
      }
    }
    // This just puts the output in a buffer, which is not part of the workUnit
    preparedOutput.produce()
  }

  // protected to allow unit-testing
  protected[morsel] def scheduleNextTask(executingQuery: ExecutingQuery, resources: QueryResources): SchedulingResult[Task[QueryResources]] = {
    try {
      schedulingPolicy.nextTask(executingQuery, resources)
    } catch {
      // Failure in nextTask of a pipeline, after taking Morsel
      case NextTaskException(pipeline, SchedulingInputException(morsel: MorselParallelizer, cause)) =>
        executingQuery.executionState.closeMorselTask(pipeline, morsel.originalForClosing)
        executingQuery.executionState.failQuery(cause, resources, pipeline)
        SchedulingResult(null, someTaskWasFilteredOut = true)

      // Failure in nextTask of a pipeline, after taking Accumulator
      case NextTaskException(pipeline, SchedulingInputException(acc: MorselAccumulator[_], cause)) =>
        executingQuery.executionState.closeAccumulatorTask(pipeline, acc)
        executingQuery.executionState.failQuery(cause, resources, pipeline)
        SchedulingResult(null, someTaskWasFilteredOut = true)

      // Failure in nextTask of a pipeline
      case NextTaskException(pipeline, cause) =>
        executingQuery.executionState.failQuery(cause, resources, pipeline)
        SchedulingResult(null, someTaskWasFilteredOut = false)

      // Failure in scheduling query
      case throwable: Throwable =>
        executingQuery.executionState.failQuery(throwable, resources, null)
        SchedulingResult(null, someTaskWasFilteredOut = false)
    }
  }

  def assertIsNotActive(): Unit = {
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
