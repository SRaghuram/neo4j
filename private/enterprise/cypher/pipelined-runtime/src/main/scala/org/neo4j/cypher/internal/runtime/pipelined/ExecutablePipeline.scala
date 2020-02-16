/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFactory
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
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
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.util.Preconditions

case class ExecutablePipeline(id: PipelineId,
                              lhs: PipelineId,
                              rhs: PipelineId,
                              start: Operator,
                              middleOperators: Array[MiddleOperator],
                              serial: Boolean,
                              slots: SlotConfiguration,
                              inputBuffer: BufferDefinition,
                              outputOperator: OutputOperator,
                              needsMorsel: Boolean = true,
                              needsFilteringMorsel: Boolean = false) extends WorkIdentity {

  def isFused: Boolean = start match {
    case _: CompiledStreamingOperator => true
    case _ => false
  }

  def createState(executionState: ExecutionState,
                  queryContext: QueryContext,
                  queryState: QueryState,
                  resources: QueryResources,
                  stateFactory: StateFactory): PipelineState = {

    // when a pipeline is fully fused (including output operator) the CompiledTask acts as OutputOperator
    def outputOperatorStateFor(operatorTask: OperatorTask): OutputOperatorState = (operatorTask, outputOperator) match {
      case (compiledTask: CompiledTask, NoOutputOperator) if middleOperators.isEmpty =>
        compiledTask.createState(executionState)
      case _ =>
        outputOperator.createState(executionState)
    }

    val middleTasks = new Array[OperatorTask](middleOperators.length)
    var i = 0
    while (i < middleTasks.length) {
      middleTasks(i) = middleOperators(i).createTask(executionState, stateFactory, queryContext, queryState, resources)
      i += 1
    }

    new PipelineState(this,
      start.createState(executionState, stateFactory, queryContext, queryState, resources),
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

  /**
   * Returns the next task for this pipeline, or `null` if no task is available.
   *
   * If a continuation of a previous task is available, that will be returned.
   * Otherwise, the start operator of the pipeline will look for new input from
   * which to generate tasks. If the start operator find input and generates tasks,
   * one of these will be returned, and the rest stored as continuations.
   */
  def nextTask(context: QueryContext,
               state: QueryState,
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
            val t = innerNextTask(context, state, resources)
            if (t == null) {
              // the data might have been taken while we took the lock
              executionState.unlock(pipeline)
            }
            t
          } else {
            null
          }
        } else {
          innerNextTask(context, state, resources)
        }
        // filterCancelledArguments checks if there is work left to do for a task
        // if it returns `true`, there is no work left and the task has been already closed.
      } while (task != null && taskCancelled(task))
    } catch {
      case NonFatalCypherError(t) =>
        if (DebugSupport.SCHEDULING.enabled)
          DebugSupport.SCHEDULING.log(s"[nextTask] failed with $t")
        else if (DebugSupport.WORKERS.enabled)
          DebugSupport.WORKERS.log(s"[nextTask] failed with $t")
        throw NextTaskException(pipeline, t)
    }
    SchedulingResult(task, someTaskWasFilteredOut)
  }

  def allocateMorsel(producingWorkUnitEvent: WorkUnitEvent, state: QueryState): MorselCypherRow = {
    // TODO: Change pipeline.needsMorsel and needsFilteringMorsel into an Option[MorselFactory]
    //       The MorselFactory should probably originate from the MorselBuffer to play well with reuse/pooling
    if (pipeline.needsMorsel) {
      val slots = pipeline.slots
      if (pipeline.needsFilteringMorsel) {
        MorselFactory.allocateFiltering(slots, state.morselSize, producingWorkUnitEvent)
      } else {
        MorselFactory.allocate(slots, state.morselSize, producingWorkUnitEvent)
      }
    } else MorselCypherRow.empty
  }

  private def innerNextTask(context: QueryContext,
                            state: QueryState,
                            resources: QueryResources): PipelineTask = {
    if (!executionState.canPut(pipeline)) {
      return null
    }
    val task = executionState.takeContinuation(pipeline)
    if (task != null) {
      return task
    }

    val parallelism = if (pipeline.serial) 1 else state.numberOfWorkers
    val startTasks = startState.nextTasks(context, state, this, parallelism, resources, executionState.argumentStateMaps)
    if (startTasks != null) {
      Preconditions.checkArgument(startTasks.nonEmpty, "If no tasks are available, `null` is expected rather than empty collections")

      def asPipelineTask(startTask: ContinuableOperatorTask): PipelineTask =
        PipelineTask(startTask,
          middleTasks,
          // output operator state may be stateful, should not be shared across tasks
          outputOperatorStateCreator(startTask),
          context,
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

  override def takeAccumulator[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](): ACC = {
    executionState.takeAccumulator(pipeline.inputBuffer.id)
  }

  override def takeAccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](): AccumulatorAndMorsel[DATA, ACC] = {
    executionState.takeAccumulatorAndMorsel(pipeline.inputBuffer.id)
  }

  override def takeData[DATA <: AnyRef](): DATA = {
    executionState.takeData(pipeline.inputBuffer.id)
  }

  /* OperatorCloser */

  override def closeMorsel(morsel: MorselCypherRow): Unit = {
    executionState.closeMorselTask(pipeline, morsel)
  }

  override def closeData[DATA <: AnyRef](data: DATA): Unit = {
    executionState.closeData(pipeline, data)
  }

  override def closeAccumulator(accumulator: MorselAccumulator[_]): Unit = {
    executionState.closeAccumulatorTask(pipeline, accumulator)
  }

  override def closeMorselAndAccumulatorTask(morsel: MorselCypherRow, accumulator: MorselAccumulator[_]): Unit = {
    executionState.closeMorselAndAccumulatorTask(pipeline, morsel, accumulator)
  }

  override def filterCancelledArguments(morsel: MorselCypherRow): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel)
  }

  override def filterCancelledArguments(accumulator: MorselAccumulator[_]): Boolean = {
    executionState.filterCancelledArguments(pipeline, accumulator)
  }

  override def filterCancelledArguments(morsel: MorselCypherRow, accumulator: MorselAccumulator[_]): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel, accumulator)
  }
}
