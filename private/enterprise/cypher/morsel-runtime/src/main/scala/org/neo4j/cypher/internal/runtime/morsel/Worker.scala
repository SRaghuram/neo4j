/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.RuntimeResourceLeakException
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
  * A worker has it's own [[WorkerExecutionResources]] which it will use to execute tasks.
  */
class Worker(val workerId: Int,
             queryManager: QueryManager,
             schedulingPolicy: SchedulingPolicy,
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
    * @return `true` if some work was performed, otherwise `false`
    */
  def workOnQuery(executingQuery: ExecutingQuery, resources: WorkerExecutionResources): Boolean = {
    val task = scheduleNextTask(executingQuery, resources)
    if (task == null) {
      false
    } else {
      try {
        task match {
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
        case throwable: Throwable =>
          try {
            task match {
              case pipelineTask: PipelineTask =>
                executingQuery.executionState.failQuery(throwable, resources, pipelineTask.pipelineState.pipeline)
                pipelineTask.close(resources)
            }
          } catch {
            case t2:Throwable =>
              // Cleaning up also failed
              throwable.addSuppressed(t2)
              // We would actually want a hard shutdown here
              throwable.printStackTrace()
          }
          true
      }
    }
  }

  private def executeTask(executingQuery: ExecutingQuery,
                          task: PipelineTask,
                          resources: WorkerExecutionResources): Unit = {
    var workUnitEvent: WorkUnitEvent = null
    var preparedOutput: PreparedOutput = null
    try {
      try {
        executingQuery.bindTransactionToThread()

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
    } finally {
      executingQuery.unbindTransaction()
    }
  }

  private def scheduleNextTask(executingQuery: ExecutingQuery, resources: WorkerExecutionResources): Task[WorkerExecutionResources] = {
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
