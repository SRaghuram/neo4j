/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.physicalplanning.{BufferDefinition, PipelineId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.execution.{Morsel, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.{Operator, OperatorState, _}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, WorkIdentity}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.util.Preconditions

case class ExecutablePipeline(id: PipelineId,
                              start: Operator,
                              middleOperators: Array[MiddleOperator],
                              serial: Boolean,
                              slots: SlotConfiguration,
                              inputBuffer: BufferDefinition,
                              outputOperator: OutputOperator) extends WorkIdentity {

  def createState(executionState: ExecutionState,
                  queryContext: QueryContext,
                  queryState: QueryState,
                  resources: QueryResources): PipelineState =

    new PipelineState(this,
                      start.createState(executionState),
                      middleOperators.map(_.createTask(executionState, queryContext, queryState, resources)),
                      () => outputOperator.createState(executionState, id),
                      executionState)

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
                    // TODO reviewer: it feels awkward that start & outputOperator are a-sci-metric (one 'state' creates tasks, while the other does not)
                    //                OutputOperator should probably have something analogous to OperatorState.newTasks()
                    outputOperatorStateCreator:() => OutputOperatorState,
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
               resources: QueryResources): PipelineTask = {
    var task: PipelineTask = null
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
      } while (task != null && task.filterCancelledArguments(resources))
    } catch {
      case t: Throwable =>
        throw NextTaskException(pipeline, t)
    }
    task
  }

  def allocateMorsel(producingWorkUnitEvent: WorkUnitEvent, state: QueryState): MorselExecutionContext = {

      val slots = pipeline.slots
      val slotSize = slots.size()
      val morsel = Morsel.create(slots, state.morselSize)
      new MorselExecutionContext(
        morsel,
        slotSize.nLongs,
        slotSize.nReferences,
        state.morselSize,
        currentRow = 0,
        slots,
        producingWorkUnitEvent)
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
    val streamTasks = startState.nextTasks(context, state, this, parallelism, resources)
    if (streamTasks != null) {
      Preconditions.checkArgument(streamTasks.nonEmpty, "If no tasks are available, `null` is expected rather than empty collections")
      val tasks = streamTasks.map(startTask => PipelineTask(startTask,
                                                            middleTasks,
                                                            // output operator state may be stateful, should not be shared across tasks
                                                            outputOperatorStateCreator(),
                                                            context,
                                                            state,
                                                            this))
      var i = 1
      while (i < tasks.length) {
        putContinuation(tasks(i), wakeUp = true, resources)
        i += 1
      }
      tasks.head
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
    executionState.takeMorsel(pipeline.inputBuffer.id, pipeline)
  }

  override def takeAccumulator[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](): ACC = {
    executionState.takeAccumulator(pipeline.inputBuffer.id, pipeline)
  }

  override def takeAccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](): AccumulatorAndMorsel[DATA, ACC] = {
    executionState.takeAccumulatorAndMorsel(pipeline.inputBuffer.id, pipeline)
  }

  override def takeData[DATA <: AnyRef](): DATA = {
    executionState.takeData(pipeline.inputBuffer.id, pipeline)
  }

  /* OperatorCloser */

  override def closeMorsel(morsel: MorselExecutionContext): Unit = {
    executionState.closeMorselTask(pipeline, morsel)
  }

  override def closeData[DATA <: AnyRef](data: DATA): Unit = {
    executionState.closeData(pipeline, data)
  }

  override def closeAccumulator(accumulator: MorselAccumulator[_]): Unit = {
    executionState.closeAccumulatorTask(pipeline, accumulator)
  }

  override def closeMorselAndAccumulatorTask(morsel: MorselExecutionContext, accumulator: MorselAccumulator[_]): Unit = {
    executionState.closeMorselAndAccumulatorTask(pipeline, morsel, accumulator)
  }

  override def filterCancelledArguments(morsel: MorselExecutionContext): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel)
  }

  override def filterCancelledArguments(accumulator: MorselAccumulator[_]): Boolean = {
    executionState.filterCancelledArguments(pipeline, accumulator)
  }

  override def filterCancelledArguments(morsel: MorselExecutionContext, accumulator: MorselAccumulator[_]): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel, accumulator)
  }
}
