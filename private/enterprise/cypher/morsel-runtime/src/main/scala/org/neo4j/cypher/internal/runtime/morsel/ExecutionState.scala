/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.{Buffer, LHSAccumulatingRHSStreamingBuffer, OptionalMorselBuffer, Sink}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, MorselParallelizer}

/**
  * Creator of [[ArgumentStateMap]].
  */
trait ArgumentStateMapCreator {

  /**
    * Create new [[ArgumentStateMap]]. Only a single [[ArgumentStateMap]] can be created for each `argumentStateMapId`.
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
    * The pipeline states of this execution.
    */
  def pipelineStates: Array[PipelineState]

  /**
    * The sink with if `bufferId` of the pipeline with id `fromPipeline`.
    */
  def getSink[T <: AnyRef](fromPipeline: PipelineId, bufferId: BufferId): Sink[T]

  /**
    * Put a morsel into the buffer with id `bufferId`.
    */
  def putMorsel(fromPipeline: PipelineId, bufferId: BufferId, morsel: MorselExecutionContext): Unit

  /**
    * Take a morsel from the buffer with id `bufferId`.
    *
    * @return the morsel to take, or `null` if no morsel was available
    */
  def takeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer

  /**
    * Take one accumulator that is ready from the argument state map buffer with id `bufferId`.
    *
    * @return the ready morsel accumulator, or `null` if no accumulators are ready
    */
  def takeAccumulator[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](bufferId: BufferId, pipeline: ExecutablePipeline): ACC

  /**
    * Take one accumulator that is ready (LHS) and a morsel (RHS) together from the [[LHSAccumulatingRHSStreamingBuffer]] with id `bufferId`.
    *
    * @return the ready morsel accumulator, or `null` if no accumulator is ready
    */
  def takeAccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](bufferId: BufferId, pipeline: ExecutablePipeline): AccumulatorAndMorsel[DATA, ACC]

  /**
    * Take data from the [[OptionalMorselBuffer]] buffer with id `bufferId`.
    *
    * @return the data to take, or `null` if no data was available
    */
  def takeData[DATA <: AnyRef](bufferId: BufferId, pipeline: ExecutablePipeline): DATA

  /**
    * Close a pipeline task which was executing over an input morsel.
    *
    * @param pipeline the executing pipeline
    * @param inputMorsel the input morsel
    */
  def closeMorselTask(pipeline: ExecutablePipeline, inputMorsel: MorselExecutionContext): Unit

  /**
    * Close a pipeline task which was executing over some data from an [[OptionalMorselBuffer]].
    * @param pipeline the executing pipeline
    * @param data the input data
    */
  def closeData[DATA <: AnyRef](pipeline: ExecutablePipeline, data: DATA): Unit

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
  def closeAccumulatorTask(pipeline: ExecutablePipeline, accumulator: MorselAccumulator[_]): Unit

  /**
    * Close a pipeline task which was executing over some input morsel accumulator (LHS) and a morsel (RHS) from a [[LHSAccumulatingRHSStreamingBuffer]].
    *
    * @param pipeline the executing pipeline
    * @param inputMorsel the input morsel
    * @param accumulator the input morsel accumulator
    */
  def closeMorselAndAccumulatorTask(pipeline: ExecutablePipeline,
                                    inputMorsel: MorselExecutionContext,
                                    accumulator: MorselAccumulator[_]): Unit

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
  def filterCancelledArguments(pipeline: ExecutablePipeline,
                               accumulator: MorselAccumulator[_]): Boolean

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
    *
    * @param pipeline the executing pipeline
    * @param inputMorsel the input morsel
    * @param accumulator the accumulator
    * @return `true` iff both the morsel and the accumulator are cancelled
    */
  def filterCancelledArguments(pipeline: ExecutablePipeline,
                               inputMorsel: MorselExecutionContext,
                               accumulator: MorselAccumulator[_]): Boolean


  /**
    * Checks if there is room in buffers to accept morsels
    * @param pipeline the current pipeline
    * @return `true` if there is room otherwise `false`
    */
  def canPut(pipeline: ExecutablePipeline): Boolean

  /**
    * Continue executing pipeline `p`.
    *
    * @return the task to continue executing, or `null` if no task was available
    */
  def takeContinuation(p: ExecutablePipeline): PipelineTask

  /**
    * Put `task` to the continuation queue for its pipeline, so we can continue executing it later.
    *
    * @param task the task that can be executed (again)
    * @param wakeUp `true` if a worker should be woken because of this put
    * @param resources resources used for closing this task if the query is failed
    */
  def putContinuation(task: PipelineTask, wakeUp: Boolean, resources: QueryResources): Unit

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
    * Fail the query.
    *
    * @param throwable the observed exception
    * @param resources resources where to hand-back any open cursors
    * @param failedPipeline pipeline what was executing while the failure occurred, or `null` if
    *                       the failure happened pipeline execution
    */
  def failQuery(throwable: Throwable, resources: QueryResources, failedPipeline: ExecutablePipeline): Unit

  /**
    * Cancel the query immediately.
    *
    * @param resources resources where to hand-back any open cursors
    */
  def cancelQuery(resources: QueryResources): Unit

  /**
    * Cancel the query as soon as possible. This will schedule a Task that performs the cancellation.
    */
  def scheduleCancelQuery(): Unit

  /**
    * Return the clean up task if one was scheduled or `null` otherwise
    */
  def cleanUpTask(): CleanUpTask

  /**
    * Check whether this query has completed. A query is completed if it has
    * produced all results, or an exception has occurred.
    */
  def isCompleted: Boolean

  /**
    * Return a string representation of the state related to the given pipeline. Meant for debugging.
    */
  def prettyString(pipeline: ExecutablePipeline): String
}
