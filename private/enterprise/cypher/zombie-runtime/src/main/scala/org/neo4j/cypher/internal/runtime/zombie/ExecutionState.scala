/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.StateDefinition
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext

import scala.collection.mutable.ArrayBuffer

/**
  * The execution state of a single executing query.
  */
trait ExecutionState {

  /**
    * Produce a new morsel into the row buffer with id `outputId`.
    */
  def produceStreamingRows(outputId: Int, morsel: MorselExecutionContext): Unit

  /**
    * Consume a morsel from the row buffer with id `outputId`.
    *
    * @return the morsel to consume, or `null` if no morsel was available
    */
  def consumeStreamingRows(id: Int): MorselExecutionContext

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
}

object SingleThreadedStateBuilder {

  def build(stateDefinition: StateDefinition,
            executablePipelines: IndexedSeq[ExecutablePipeline]): ExecutionState = new SingleThreadedState(stateDefinition.bufferCount, executablePipelines.size)
}

class SingleThreadedState(bufferCount: Int, pipelineCount: Int) extends ExecutionState {

  private val buffers = new Array[ArrayBuffer[MorselExecutionContext]](bufferCount).map(i => new ArrayBuffer[MorselExecutionContext]())
  private val continuations = new Array[ArrayBuffer[PipelineTask]](bufferCount).map(i => new ArrayBuffer[PipelineTask]())

  override def produceStreamingRows(outputId: Int,
                                    output: MorselExecutionContext): Unit =
    buffers(outputId).append(output)

  override def consumeStreamingRows(id: Int): MorselExecutionContext = {
    if (buffers(id).isEmpty) null
    else buffers(id).remove(0)
  }

  override def addContinuation(task: PipelineTask): Unit =
    continuations(task.pipeline.id).append(task)

  override def continue(p: ExecutablePipeline): PipelineTask = {
    if (continuations(p.id).isEmpty) null
    else continuations(p.id).remove(0)
  }

  override def initialize(): Unit = {
    buffers.head += MorselExecutionContext.createSingleRow()
  }
}
