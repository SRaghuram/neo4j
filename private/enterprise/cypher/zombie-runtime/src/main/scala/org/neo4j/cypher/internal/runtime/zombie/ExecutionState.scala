/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.{Buffer, LHSAccumulatingRHSStreamingBuffer}
import org.neo4j.cypher.internal.runtime.zombie.state.{ArgumentStateMap, MorselParallelizer}

/**
  * Creator of [[ArgumentStateMap]].
  */
trait ArgumentStateMapCreator {

  /**
    * Create new [[ArgumentStateMap]]. Only a single [[ArgumentStateMap]] can be created for each `argumentStateDefinition`.
    */
  def createArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                 factory: ArgumentStateFactory[S]): ArgumentStateMap[S]

  /**
    * Create a new [[Buffer]]
    */
  def newBuffer[T <: AnyRef](): Buffer[T]
}

/**
  * The execution state of a single executing query.
  */
trait ExecutionState extends ArgumentStateMapCreator {

  /**
    * Put a morsel into the buffer with id `bufferId`.
    */
  def putMorsel(fromPipeline: PipelineId, bufferId: BufferId, morsel: MorselExecutionContext): Unit

  /**
    * Take a morsel from the row buffer with id `bufferId`.
    *
    * @return the morsel to take, or `null` if no morsel was available
    */
  def takeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer

  /**
    * Take one accumulator that is ready from the argument state map buffer with id `bufferId`.
    *
    * @return the ready morsel accumulator, or `null` if no accumulators are ready
    */
  def takeAccumulator[ACC <: MorselAccumulator](bufferId: BufferId, pipeline: ExecutablePipeline): ACC

  /**
    * Take one accumulator that is ready (LHS) and a morsel (RHS) together from the [[LHSAccumulatingRHSStreamingBuffer]] with id `bufferId`.
    *
    * @return the ready morsel accumulator, or `null` if no accumulators are ready
    */
  def takeAccumulatorAndMorsel[ACC <: MorselAccumulator](bufferId: BufferId, pipeline: ExecutablePipeline): (ACC, MorselExecutionContext)

  /**
    * Close a pipeline task which was executing over an input morsel.
    *
    * @param pipeline the executing pipeline
    * @param inputMorsel the input morsel
    */
  def closeMorselTask(pipeline: ExecutablePipeline, inputMorsel: MorselExecutionContext): Unit

  /**
    * Close the work unit of a pipeline. This will release a lock
    * if the pipeline is serial. Otherwise this does nothing.
    *
    * This is called from all close... methods in [[ExecutionState]], so if your're calling these
    * methods there is no need to call this method as well
    *
    * It as also called from [[filterCancelledArguments()]] if a task is filtered out completely.
    */
  def closeWorkUnit(pipeline: ExecutablePipeline): Unit

  /**
    * Close a pipeline task which was executing over some input morsel accumulator.
    *
    * @param pipeline the executing pipeline
    * @param accumulator the input morsel accumulator
    */
  def closeAccumulatorTask[ACC <: MorselAccumulator](pipeline: ExecutablePipeline, accumulator: ACC): Unit

  /**
    * Close a pipeline task which was executing over some input morsel accumulator (LHS) and a morsel (RHS) from a [[LHSAccumulatingRHSStreamingBuffer]].
    *
    * @param pipeline the executing pipeline
    * @param inputMorsel the input morsel
    * @param accumulator the input morsel accumulator
    */
  def closeMorselAndAccumulatorTask[ACC <: MorselAccumulator](pipeline: ExecutablePipeline,
                                                              inputMorsel: MorselExecutionContext,
                                                              accumulator: ACC): Unit

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    *
    * @param pipeline    the executing pipeline
    * @param inputMorsel the input morsel
    * @return `true` iff the morsel is cancelled
    */
  def filterCancelledArguments(pipeline: ExecutablePipeline, inputMorsel: MorselExecutionContext): Boolean

  /**
    * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
    *
    * @param pipeline     the executing pipeline
    * @param accumulator the accumulator
    * @return `true` iff the accumulator is cancelled
    */
  def filterCancelledArguments[ACC <: MorselAccumulator](pipeline: ExecutablePipeline,
                                                         accumulator: ACC): Boolean

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
    *
    * @param pipeline the executing pipeline
    * @param inputMorsel the input morsel
    * @param accumulator the accumulator
    * @return `true` iff both the morsel and the accumulator are cancelled
    */
  def filterCancelledArguments[ACC <: MorselAccumulator](pipeline: ExecutablePipeline,
                                                         inputMorsel: MorselExecutionContext,
                                                         accumulator: ACC): Boolean

  /**
    * Continue executing pipeline `p`.
    *
    * @return the task to continue executing, or `null` if no task was available
    */
  def takeContinuation(p: ExecutablePipeline): PipelineTask

  /**
    * Put `task` to the continuation queue for its pipeline, so we can continue executing it later.
    */
  def putContinuation(task: PipelineTask): Unit

  /**
    * Try to lock execution of the given pipeline.
    *
    * @return `true` iff the pipeline was locked
    */
  def tryLock(pipeline: ExecutablePipeline): Boolean

  /**
    * Unlock execution of the given pipeline.
    */
  def unlock(pipeline: ExecutablePipeline): Unit

  /**
    * Check if the pipeline can execute either a continuation or a new task.
    *
    * @return `true` if the pipeline can be executed
    */
  def canContinueOrTake(pipeline: ExecutablePipeline): Boolean

  /**
    * Adds an empty row to the initBuffer.
    */
  def initializeState(): Unit

  /**
    * Await the completion of this query execution.
    */
  def awaitCompletion(): Unit

  /**
    * Check whether this query has completed.
    */
  def isCompleted: Boolean

  /**
    * Return a string representation of the state related to the given pipeline. Meant for debugging.
    */
  def prettyString(pipeline: ExecutablePipeline): String
}
