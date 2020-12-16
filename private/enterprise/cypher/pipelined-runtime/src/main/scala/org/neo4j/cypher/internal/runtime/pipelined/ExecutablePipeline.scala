/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.PipelineState.computeMorselSize
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFactory
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.CompiledStreamingOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.CompiledTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.MiddleOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NoOutputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.Operator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCloser
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorInput
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.OutputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OutputOperatorState
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeHeadOperator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndPayload
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

case class ExecutablePipeline(id: PipelineId,
                              lhs: PipelineId,
                              rhs: PipelineId,
                              start: Operator,
                              middleOperators: Array[MiddleOperator],
                              serial: Boolean,
                              slots: SlotConfiguration,
                              inputBuffer: BufferDefinition,
                              outputOperator: OutputOperator,
                              workLimiter: Option[ArgumentStateMapId],
                              needsMorsel: Boolean = true,
                              needsFilteringMorsel: Boolean = false) extends WorkIdentity {

  def startOperatorCanTrackTime: Boolean = start match {
    case _: CompiledStreamingOperator => false
    case _: SlottedPipeHeadOperator => false
    case _ => true
  }

  def createState(executionState: ExecutionState,
                  queryState: PipelinedQueryState,
                  resources: QueryResources,
                  stateFactory: StateFactory): PipelineState = {

    // when a pipeline is fully fused (including output operator) the CompiledTask acts as OutputOperator
    def outputOperatorStateFor(operatorTask: OperatorTask): OutputOperatorState = (operatorTask, outputOperator) match {
      case (compiledTask: CompiledTask, NoOutputOperator) if middleOperators.isEmpty =>
        compiledTask.createState(executionState, stateFactory)
      case (compiledTask: CompiledTask, _) =>
        compiledTask.createState(executionState, stateFactory) // Always call createState of the compiled task even if not used as the actual output
        outputOperator.createState(executionState, stateFactory)
      case _ =>
        outputOperator.createState(executionState, stateFactory)
    }

    val middleTasks = new Array[OperatorTask](middleOperators.length)
    var i = 0
    while (i < middleTasks.length) {
      middleTasks(i) = middleOperators(i).createTask(executionState, stateFactory, queryState, resources)
      i += 1
    }

    new PipelineState(this,
      start.createState(executionState, stateFactory, queryState, resources),
      middleTasks,
      outputOperatorStateFor,
      executionState)
  }

  override val workId: Id = start.workIdentity.workId
  override val workDescription: String = composeWorkDescriptions(start, middleOperators, outputOperator)

  private def composeWorkDescriptions(first: HasWorkIdentity, others: Seq[HasWorkIdentity], last: HasWorkIdentity): String = {
    val workIdentities = Seq(first) ++ others ++ Seq(last)
    s"${workIdentities.map(_.workIdentity.workDescription).mkString(",")}"
  }

  override def toString: String = workDescription
}

class PipelineState(val pipeline: ExecutablePipeline,
                    startState: OperatorState,
                    middleTasks: Array[OperatorTask],
                    outputOperatorStateCreator: OperatorTask => OutputOperatorState,
                    executionState: ExecutionState) extends OperatorInput with OperatorCloser {

  private val workLimiterASM =
    pipeline.workLimiter match {
      case None => null
      case Some(id) =>
        executionState.argumentStateMaps(id).asInstanceOf[ArgumentStateMap[WorkCanceller]]
    }

  /**
   * Returns the next task for this pipeline, or `null` if no task is available.
   *
   * If a continuation of a previous task is available, that will be returned.
   * Otherwise, the start operator of the pipeline will look for new input from
   * which to generate tasks. If the start operator find input and generates tasks,
   * one of these will be returned, and the rest stored as continuations.
   */
  def nextTask(state: PipelinedQueryState,
               resources: QueryResources): SchedulingResult[PipelineTask] = {
    var task: PipelineTask = null
    var someTaskWasFilteredOut = false

    def taskCancelled(task: PipelineTask): Boolean = {
      val cancelled = task.filterCancelledArguments(resources)
      if (cancelled) {
        someTaskWasFilteredOut = true
      }
      cancelled
    }

    try {
      // Loop until we find a task that is not completely cancelled
      do {
        task = if (pipeline.serial) {
          if (executionState.canContinueOrTake(pipeline) && executionState.tryLock(pipeline)) {
            val t = innerNextTask(state, resources)
            if (t == null) {
              // the data might have been taken while we took the lock
              executionState.unlock(pipeline)
            }
            t
          } else {
            null
          }
        } else {
          innerNextTask(state, resources)
        }
        // filterCancelledArguments checks if there is work left to do for a task
        // if it returns `true`, there is no work left and the task has been already closed.
      } while (task != null && taskCancelled(task))
    } catch {
      case NonFatalCypherError(t) =>
        if (DebugSupport.SCHEDULING.enabled) {
          DebugSupport.SCHEDULING.log(s"[nextTask] failed with $t")
        } else if (DebugSupport.WORKERS.enabled) {
          DebugSupport.WORKERS.log(s"[nextTask] failed with $t")
        }
        throw NextTaskException(pipeline, t)
    }
    SchedulingResult(task, someTaskWasFilteredOut)
  }

  def allocateMorsel(producingWorkUnitEvent: WorkUnitEvent, state: PipelinedQueryState): Morsel = {
    // TODO: Change pipeline.needsMorsel and needsFilteringMorsel into an Option[MorselFactory]
    //       The MorselFactory should probably originate from the MorselBuffer to play well with reuse/pooling
    if (pipeline.needsMorsel) {
      val slots = pipeline.slots
      val morselSize = computeMorselSize(remainingRows, state.morselSize)
      if (pipeline.needsFilteringMorsel) {
        MorselFactory.allocateFiltering(slots, morselSize, producingWorkUnitEvent)
      } else {
        MorselFactory.allocate(slots, morselSize, producingWorkUnitEvent)
      }
    } else {
      Morsel.empty
    }
  }

  private def remainingRows: Long =
    if (workLimiterASM != null)
      workLimiterASM.peek(TopLevelArgument.VALUE).remaining
    else
      Long.MaxValue

  private def innerNextTask(state: PipelinedQueryState,
                            resources: QueryResources): PipelineTask = {
    if (!executionState.canPut(pipeline)) {
      if (DebugSupport.SCHEDULING.enabled) {
        DebugSupport.SCHEDULING.log(s"[nextTask] pipeline cannot put! ($pipeline)")
      }
      return null
    }
    val task = executionState.takeContinuation(pipeline)
    if (task != null) {
      return task
    }

    val parallelism = if (pipeline.serial) 1 else state.numberOfWorkers
    val startTasks = startState.nextTasks(state, this, parallelism, resources, executionState.argumentStateMaps)
    if (startTasks != null) {
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(startTasks.nonEmpty, "If no tasks are available, `null` is expected rather than empty collections")

      def asPipelineTask(startTask: ContinuableOperatorTask): PipelineTask =
        PipelineTask(startTask,
          middleTasks,
          // output operator state may be stateful, should not be shared across tasks
          outputOperatorStateCreator(startTask),
          state,
          this)

      var i = 1
      while (i < startTasks.size) {
        putContinuation(asPipelineTask(startTasks(i)), wakeUp = true, resources)
        i += 1
      }
      asPipelineTask(startTasks(0))

    } else {
      null
    }
  }

  /**
   * If a task can continue, or multiple parallel tasks for a pipeline are obtained at once,
   * this method it will be placed in the continuation queue for this pipeline.
   *
   * @param task the task that can be executed (again)
   * @param wakeUp `true` if a worker should be woken because of this put
   * @param resources resources used for closing this task if the query is failed
   */
  def putContinuation(task: PipelineTask, wakeUp: Boolean, resources: QueryResources): Unit = {
    executionState.putContinuation(task, wakeUp, resources)
  }

  /* OperatorInput */

  override def takeMorsel(): MorselParallelizer = {
    executionState.takeMorsel(pipeline.inputBuffer.id)
  }

  override def takeAccumulators[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](n: Int): IndexedSeq[ACC] = {
    executionState.takeAccumulators(pipeline.inputBuffer.id, n)
  }

  override def takeAccumulatorAndPayload[ACC_DATA <: AnyRef, ACC <: MorselAccumulator[ACC_DATA], PAYLOAD <: AnyRef](): AccumulatorAndPayload[ACC_DATA, ACC, PAYLOAD] = {
    executionState.takeAccumulatorAndPayload(pipeline.inputBuffer.id)
  }

  override def takeData[DATA <: AnyRef](): DATA = {
    executionState.takeData(pipeline.inputBuffer.id)
  }

  /* OperatorCloser */

  override def closeMorsel(morsel: Morsel): Unit = {
    executionState.closeMorselTask(pipeline, morsel)
  }

  override def closeData[DATA <: AnyRef](data: DATA): Unit = {
    executionState.closeData(pipeline, data)
  }

  override def closeAccumulators(accumulators: IndexedSeq[MorselAccumulator[_]]): Unit = {
    executionState.closeAccumulatorsTask(pipeline, accumulators)
  }

  override def closeDataAndAccumulatorTask[PAYLOAD <: AnyRef](data: PAYLOAD, accumulator: MorselAccumulator[_]): Unit = {
    executionState.closeDataAndAccumulatorTask(pipeline, data, accumulator)
  }

  override def filterCancelledArguments(morsel: Morsel): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel)
  }

  override def filterCancelledArguments(accumulator: MorselAccumulator[_]): Boolean = {
    executionState.filterCancelledArguments(pipeline, accumulator)
  }

  override def filterCancelledArguments(morsel: Morsel, accumulator: MorselAccumulator[_]): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel, accumulator)
  }
}

object PipelineState {
  val MIN_MORSEL_SIZE: Int = 10

  def computeMorselSize(remainingRows: Long, configuredMorselSize: Int): Int = {
    //lowerBound is MIN_MORSEL_SIZE, if not explicitly configured to be lower
    val lowerBound = math.min(MIN_MORSEL_SIZE, configuredMorselSize)
    //upperBound is what remains from the limit as long as that is below the configured morselSize
    val upperBound = math.min(remainingRows, configuredMorselSize).toInt
    math.max(upperBound, lowerBound)
  }
}
