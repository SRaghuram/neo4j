/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Creator of [[ArgumentStateMap]].
  */
trait ArgumentStateCreator {

  /**
    * Create new [[ArgumentStateMap]]. Only a single [[ArgumentStateMap]] can be created for each `reducePlanId`.
    */
  def createArgumentStateMap[S <: ArgumentState](reducePlanId: Id,
                                                 factory: ArgumentStateFactory[S]): ArgumentStateMap[S]
}

/**
  * The execution state of a single executing query.
  */
trait ExecutionState extends ArgumentStateCreator {

  /**
    * Put a morsel into the buffer with id `bufferId`.
    */
  def putMorsel(bufferId: BufferId, morsel: MorselExecutionContext): Unit

  /**
    * Take a morsel from the row buffer with id `bufferId`.
    *
    * @return the morsel to take, or `null` if no morsel was available
    */
  def takeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer

  /**
    * Take all morsel accumulators that are ready from the argument state map buffer with id `bufferId`.
    *
    * @return the ready morsel accumulators, or `null` if no accumulators are ready
    */
  def takeAccumulators[ACC <: MorselAccumulator](bufferId: BufferId, pipeline: ExecutablePipeline): Iterable[ACC]

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
    * Close a pipeline task which was executing over some input morsel accumulators.
    *
    * @param pipeline the executing pipeline
    * @param accumulators the input morsel accumulators
    */
  def closeAccumulatorsTask[ACC <: MorselAccumulator](pipeline: ExecutablePipeline, accumulators: Iterable[ACC]): Unit

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    *
    * @param pipeline    the executing pipeline
    * @param inputMorsel the input morsel
    * @return `true` iff the morsel is cancelled
    */
  def filterCancelledArguments(pipeline: ExecutablePipeline, inputMorsel: MorselExecutionContext): Boolean

  /**
    * Remove all accumulators related to cancelled argumentRowIds from `accumulators`.
    *
    * @param pipeline     the executing pipeline
    * @param accumulators the input morsel accumulators
    * @return `true` iff the morsel is cancelled
    */
  def filterCancelledArguments[ACC <: MorselAccumulator](pipeline: ExecutablePipeline,
                                                         accumulators: Iterable[ACC]): Boolean

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
