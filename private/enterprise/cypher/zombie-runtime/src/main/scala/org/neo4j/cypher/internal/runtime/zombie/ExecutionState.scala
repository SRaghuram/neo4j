/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.{BufferId, PipelineId}
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
  def createArgumentStateMap[T <: MorselAccumulator](reducePlanId: Id,
                                                     constructor: () => T
                                                    ): ArgumentStateMap[T]
}

/**
  * The execution state of a single executing query.
  */
trait ExecutionState extends ArgumentStateCreator {

  /**
    * Get the [[PipelineState]] of this query execution for the given `pipelineId`.
    */
  def pipelineState(pipelineId: PipelineId): PipelineState

  /**
    * Produce a morsel into the row buffer with id `bufferId`. This call
    * also resets the morsel current row to the first row.
    */
  def produceMorsel(bufferId: BufferId, morsel: MorselExecutionContext): Unit

  /**
    * Consume a morsel from the row buffer with id `bufferId`.
    *
    * @return the morsel to consume, or `null` if no morsel was available
    */
  def consumeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer

  /**
    * Close this task, meaning that we are done executing it.
    *
    * @param task the task to close
    */
  def closeTask(task: PipelineTask): Unit

  /**
    * Continue executing pipeline `p`.
    *
    * @return the task to continue executing, or `null` if no task was available
    */
  def continue(p: ExecutablePipeline): PipelineTask

  /**
    * Add `task` to the continuation queue for its pipeline, so we can continue executing it later.
    */
  def addContinuation(task: PipelineTask): Unit

  /**
    * Adds an empty row to the initBuffer.
    */
  def initialize(): Unit

  /**
    * Await the completion of this query execution.
    */
  def awaitCompletion(): Unit

  /**
    * Check whether this query has completed.
    */
  def isCompleted: Boolean
}
