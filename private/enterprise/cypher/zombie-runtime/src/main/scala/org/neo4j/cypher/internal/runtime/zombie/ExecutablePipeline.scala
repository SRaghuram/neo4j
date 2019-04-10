/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.{BufferDefinition, PipelineId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, WorkIdentity}
import org.neo4j.cypher.internal.runtime.zombie.operators.{Operator, OperatorState, ProduceResultOperator, _}
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.util.Preconditions

case class ExecutablePipeline(id: PipelineId,
                              start: Operator,
                              middleOperators: Array[MiddleOperator],
                              produceResult: Option[ProduceResultOperator],
                              serial: Boolean,
                              slots: SlotConfiguration,
                              inputBuffer: BufferDefinition,
                              outputBuffer: BufferDefinition) extends WorkIdentity {

  def createState(executionState: ExecutionState,
                  queryContext: QueryContext,
                  queryState: QueryState,
                  resources: QueryResources): PipelineState =

    new PipelineState(this,
                      start.createState(executionState),
                      middleOperators.map(_.createTask(executionState, queryContext, queryState, resources)),
                      executionState)

  override val workId: Int = start.workIdentity.workId
  override val workDescription: String = composeWorkDescriptions(start, middleOperators ++ produceResult)

  private def composeWorkDescriptions(first: HasWorkIdentity, others: Seq[HasWorkIdentity]): String = {
    val workIdentities = Seq(first) ++ others
    s"${workIdentities.map(_.workIdentity.workDescription).mkString(",")}"
  }

  override def toString: String = workDescription
}

class PipelineState(val pipeline: ExecutablePipeline,
                    startState: OperatorState,
                    middleTasks: Array[OperatorTask],
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
    } while (task != null && task.filterCancelledArguments())
    task
  }

  private def innerNextTask(context: QueryContext,
                            state: QueryState,
                            resources: QueryResources): PipelineTask = {
    val task = executionState.takeContinuation(pipeline)
    if (task != null) {
      return task
    }

    val streamTasks = startState.nextTasks(context, state, this, resources)
    if (streamTasks != null) {
      Preconditions.checkArgument(streamTasks.nonEmpty, "If no tasks are available, `null` is expected rather than empty collections")
      val produceResultsTask = pipeline.produceResult.map(_.init(context, state, resources)).orNull
      val tasks = streamTasks.map(startTask => PipelineTask(startTask,
                                                            middleTasks,
                                                            produceResultsTask,
                                                            context,
                                                            state,
                                                            this))
      for (task <- tasks.tail)
        putContinuation(task)
      tasks.head
    } else {
      null
    }
  }

  /**
    * Put the result of an execution of this pipeline into the corresponding output buffer.
    * @param morsel the output morsel
    */
  def produce(morsel: MorselExecutionContext): Unit = {
    if (pipeline.outputBuffer != null) {
      executionState.putMorsel(pipeline.id, pipeline.outputBuffer.id, morsel)
    }
  }

  /**
    * If a task can continue, or multiple parallel tasks for a pipeline are obtained at once,
    * this method it will be placed in the continuation queue for this pipeline.
    * @param task the task that can be executed (again),
    */
  def putContinuation(task: PipelineTask): Unit = {
    executionState.putContinuation(task)
  }

  /**
    * When a task is done, whether it can continue or not,
    * the work unit is done and needs to be closed.
    *
    * This will release a lock if the pipeline is serial. Otherwise this does nothing.
    */
  def closeWorkUnit(): Unit = {
    executionState.closeWorkUnit(pipeline)
  }

  /* OperatorInput */

  override def takeMorsel(): MorselParallelizer = {
    executionState.takeMorsel(pipeline.inputBuffer.id, pipeline)
  }

  override def takeAccumulator[ACC <: MorselAccumulator](): ACC = {
    executionState.takeAccumulator(pipeline.inputBuffer.id, pipeline)
  }

  override def takeAccumulatorAndMorsel[ACC <: MorselAccumulator](): (ACC, MorselExecutionContext) = {
    executionState.takeAccumulatorAndMorsel(pipeline.inputBuffer.id, pipeline)
  }

  /* OperatorCloser */

  override def closeMorsel(morsel: MorselExecutionContext): Unit = {
    executionState.closeMorselTask(pipeline, morsel)
  }

  override def closeAccumulator[ACC <: MorselAccumulator](accumulator: ACC): Unit = {
    executionState.closeAccumulatorTask(pipeline, accumulator)
  }

  override def closeMorselAndAccumulatorTask[ACC <: MorselAccumulator](morsel: MorselExecutionContext, accumulator: ACC): Unit = {
    executionState.closeMorselAndAccumulatorTask(pipeline, morsel, accumulator)
  }

  override def filterCancelledArguments(morsel: MorselExecutionContext): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel)
  }

  override def filterCancelledArguments[ACC <: MorselAccumulator](accumulator: ACC): Boolean = {
    executionState.filterCancelledArguments(pipeline, accumulator)
  }

  override def filterCancelledArguments[ACC <: MorselAccumulator](morsel: MorselExecutionContext, accumulator: ACC): Boolean = {
    executionState.filterCancelledArguments(pipeline, morsel, accumulator)
  }
}
