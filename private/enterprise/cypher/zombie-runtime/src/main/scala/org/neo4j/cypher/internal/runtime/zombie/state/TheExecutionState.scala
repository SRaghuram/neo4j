/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.runtime.zombie.execution.WorkerWaker
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory, ArgumentStateMaps, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.{Buffer, Buffers}
import org.neo4j.util.Preconditions


object TheExecutionState {
  def build(stateDefinition: StateDefinition,
            executablePipelines: Seq[ExecutablePipeline],
            stateFactory: StateFactory,
            workerWaker: WorkerWaker): ExecutionState =
    new TheExecutionState(stateDefinition.buffers, executablePipelines, stateDefinition.argumentStateMaps, stateDefinition.physicalPlan, stateFactory, workerWaker)
}

/**
  * Implementation of [[ExecutionState]].
  */
class TheExecutionState(bufferDefinitions: IndexedSeq[BufferDefinition],
                        pipelines: Seq[ExecutablePipeline],
                        argumentStateDefinitions: IndexedSeq[ArgumentStateDefinition],
                        physicalPlan: PhysicalPlan,
                        stateFactory: StateFactory,
                        workerWaker: WorkerWaker) extends ExecutionState {

  private val tracker = stateFactory.newTracker()

  // Verify that IDs and offsets match

  for (i <- pipelines.indices)
    Preconditions.checkState(i == pipelines(i).id.x, "Pipeline id does not match offset!")

  for (i <- bufferDefinitions.indices)
    Preconditions.checkState(i == bufferDefinitions(i).id.x, "Buffer definition id does not match offset!")

  // State

  private var pipelineLocks: Array[Lock] = _
  private var buffers: Buffers = _
  private val argumentStateMaps = new Array[ArgumentStateMap[_ <: ArgumentState]](argumentStateDefinitions.size)
  private var continuations: Array[Buffer[PipelineTask]] = _

  override def initializeState(): Unit = {
    pipelineLocks =
      for (pipeline <- pipelines.toArray) yield {
        stateFactory.newLock(s"Pipeline[${pipeline.id.x}]")
      }

    val asms: ArgumentStateMaps = id => argumentStateMaps(id.x)

    buffers = new Buffers(bufferDefinitions, tracker, asms, stateFactory)
    continuations = pipelines.map(_ => stateFactory.newBuffer[PipelineTask]()).toArray

    // Assumption: Buffer with ID 0 is the initual buffer
    putMorsel(NO_PIPELINE, BufferId(0), MorselExecutionContext.createInitialRow())
  }

  // Methods

  override def putMorsel(fromPipeline: PipelineId,
                         bufferId: BufferId,
                         output: MorselExecutionContext): Unit = {
    buffers.sink(fromPipeline, bufferId).put(output)
    workerWaker.wakeAll()
  }

  override def takeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer = {
    buffers.morselBuffer(bufferId).take()
  }

  override def takeAccumulator[ACC <: MorselAccumulator](bufferId: BufferId, pipeline: ExecutablePipeline): ACC = {
    buffers.source[ACC](bufferId).take()
  }

  override def takeAccumulatorAndMorsel[ACC <: MorselAccumulator](bufferId: BufferId, pipeline: ExecutablePipeline): AccumulatorAndMorsel[ACC] = {
    buffers.source[AccumulatorAndMorsel[ACC]](bufferId).take()
  }

  override def closeWorkUnit(pipeline: ExecutablePipeline): Unit = {
    if (pipeline.serial) {
      pipelineLocks(pipeline.id.x).unlock()
    }
  }

  override def closeMorselTask(pipeline: ExecutablePipeline, inputMorsel: MorselExecutionContext): Unit = {
    closeWorkUnit(pipeline)
    buffers.morselBuffer(pipeline.inputBuffer.id).close(inputMorsel)
  }

  override def closeAccumulatorTask[ACC <: MorselAccumulator](pipeline: ExecutablePipeline, accumulator: ACC): Unit = {
    closeWorkUnit(pipeline)
    buffers.argumentStateBuffer(pipeline.inputBuffer.id).close(accumulator)
  }

  override def closeMorselAndAccumulatorTask[ACC <: MorselAccumulator](pipeline: ExecutablePipeline,
                                                                       inputMorsel: MorselExecutionContext,
                                                                       accumulator: ACC): Unit = {
    closeWorkUnit(pipeline)
    val buffer = buffers.lhsAccumulatingRhsStreamingBuffer[ACC](pipeline.inputBuffer.id)
    buffer.close(accumulator, inputMorsel)
  }

  override def filterCancelledArguments(pipeline: ExecutablePipeline,
                                        inputMorsel: MorselExecutionContext): Boolean = {
    val isCancelled = buffers.morselBuffer(pipeline.inputBuffer.id).filterCancelledArguments(inputMorsel)
    if (isCancelled)
      closeWorkUnit(pipeline)
    isCancelled
  }

  override def filterCancelledArguments[ACC <: MorselAccumulator](pipeline: ExecutablePipeline,
                                                                  accumulator: ACC): Boolean = {
    val isCancelled = buffers.argumentStateBuffer[ACC](pipeline.inputBuffer.id).filterCancelledArguments(accumulator)
    if (isCancelled)
      closeWorkUnit(pipeline)
    isCancelled
  }

  override def filterCancelledArguments[ACC <: MorselAccumulator](pipeline: ExecutablePipeline,
                                                                  inputMorsel: MorselExecutionContext,
                                                                  accumulator: ACC): Boolean = {
    val buffer = buffers.lhsAccumulatingRhsStreamingBuffer[ACC](pipeline.inputBuffer.id)
    val isCancelled = buffer.filterCancelledArguments(accumulator, inputMorsel)
    if (isCancelled)
      closeWorkUnit(pipeline)
    isCancelled
  }

  override def putContinuation(task: PipelineTask): Unit = {
    continuations(task.pipelineState.pipeline.id.x).put(task)
    if (!task.pipelineState.pipeline.serial) {
      // We only wake up other Threads if this pipeline is not serial.
      // Otherwise they will all race to get this continuation while
      // this Thread can just as well continue on its own.
      workerWaker.wakeAll()
    }
  }

  override def takeContinuation(pipeline: ExecutablePipeline): PipelineTask = {
    continuations(pipeline.id.x).take()
  }

  override def tryLock(pipeline: ExecutablePipeline): Boolean = pipelineLocks(pipeline.id.x).tryLock()

  override def unlock(pipeline: ExecutablePipeline): Unit = pipelineLocks(pipeline.id.x).unlock()

  override def canContinueOrTake(pipeline: ExecutablePipeline): Boolean = {
    continuations(pipeline.id.x).hasData || buffers.hasData(pipeline.inputBuffer.id)
  }

  override final def createArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                                factory: ArgumentStateFactory[S]): ArgumentStateMap[S] = {
    val argumentSlotOffset = argumentStateDefinitions(argumentStateMapId.x).argumentSlotOffset
    val asm = stateFactory.newArgumentStateMap(argumentStateMapId, argumentSlotOffset, factory)
    argumentStateMaps(argumentStateMapId.x) = asm
    asm
  }

  override def awaitCompletion(): Unit = tracker.await()

  override def isCompleted: Boolean = tracker.isCompleted

  override def prettyString(pipeline: ExecutablePipeline): String = {
    s"""continuations: ${continuations(pipeline.id.x)}
       |""".stripMargin
  }

  // used by join operator, to create thread-safe argument states in its RHS ASM
  override def newBuffer[T <: AnyRef](): Buffer[T] =
    stateFactory.newBuffer()
}
