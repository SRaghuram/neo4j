/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.runtime.zombie.execution.{AlarmSink, WorkerWaker}
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.{Buffer, Buffers, Sink}
import org.neo4j.util.Preconditions

/**
  * Implementation of [[ExecutionState]].
  */
class TheExecutionState(stateDefinition: StateDefinition,
                        pipelines: Seq[ExecutablePipeline],
                        stateFactory: StateFactory,
                        workerWaker: WorkerWaker,
                        queryContext: QueryContext,
                        queryState: QueryState,
                        resources: QueryResources) extends ExecutionState {

  private val tracker = stateFactory.newTracker()

  // Verify that IDs and offsets match

  for (i <- pipelines.indices)
    Preconditions.checkState(i == pipelines(i).id.x, "Pipeline id does not match offset!")

  for (i <- stateDefinition.buffers.indices)
    Preconditions.checkState(i == stateDefinition.buffers(i).id.x, "Buffer definition id does not match offset!")

  // State

  // Execution state building works as a single sweep pass over the execution DAG, starting
  // from the produce results pipeline and moving towards the first pipeline and initial buffer.
  //
  // Each pipeline and buffer state is built as we traverse the DAG, meaning that pipelines
  // can acquire direct references to their sinks, and buffers can acquire references to
  // downstream buffers that they have to reference count for, and to the argument state maps
  // of any reducing operators.

  private val argumentStateMaps = new Array[ArgumentStateMap[_ <: ArgumentState]](stateDefinition.argumentStateMaps.size)
  private val buffers: Buffers = new Buffers(stateDefinition.buffers.size,
                                             tracker,
                                             id => argumentStateMaps(id.x),
                                             stateFactory)

  override val pipelineStates: Array[PipelineState] = {
    val states = new Array[PipelineState](pipelines.length)
    var i = states.length - 1
    while (i >= 0) {
      val pipeline = pipelines(i)
      pipeline.outputOperator.outputBuffer.foreach(bufferId =>
                                                   buffers.constructBuffer(stateDefinition.buffers(bufferId.x)))
      states(i) = pipeline.createState(this, queryContext, queryState, resources)
      buffers.constructBuffer(pipeline.inputBuffer)
      i -= 1
    }
    // We don't reach the first apply buffer because it is not the output buffer of any pipeline, and also
    // not the input buffer, because all apply buffers are consumed through delegates.
    buffers.constructBuffer(stateDefinition.buffers.head)
    states
  }

  private val pipelineLocks: Array[Lock] =
    for (pipeline <- pipelines.toArray) yield {
      stateFactory.newLock(s"Pipeline[${pipeline.id.x}]")
    }

  private val continuations: Array[Buffer[PipelineTask]] =
    pipelines.map(_ => stateFactory.newBuffer[PipelineTask]()).toArray

  override def initializeState(): Unit = {
    // Assumption: Buffer with ID 0 is the initial buffer
    putMorsel(NO_PIPELINE, BufferId(0), MorselExecutionContext.createInitialRow())
  }

  // Methods

  override def getSink[T <: AnyRef](fromPipeline: PipelineId,
                                    bufferId: BufferId): Sink[T] = {
    new AlarmSink(buffers.sink[T](fromPipeline, bufferId), workerWaker)
  }

  override def putMorsel(fromPipeline: PipelineId,
                         bufferId: BufferId,
                         output: MorselExecutionContext): Unit = {
    buffers.sink[MorselExecutionContext](fromPipeline, bufferId).put(output)
    workerWaker.wakeAll()
  }

  override def takeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer = {
    buffers.morselBuffer(bufferId).take()
  }

  override def takeAccumulator[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](bufferId: BufferId, pipeline: ExecutablePipeline): ACC = {
    buffers.source[ACC](bufferId).take()
  }

  override def takeAccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](bufferId: BufferId, pipeline: ExecutablePipeline): AccumulatorAndMorsel[DATA, ACC] = {
    buffers.source[AccumulatorAndMorsel[DATA, ACC]](bufferId).take()
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

  override def closeAccumulatorTask(pipeline: ExecutablePipeline, accumulator: MorselAccumulator[_]): Unit = {
    closeWorkUnit(pipeline)
    buffers.argumentStateBuffer(pipeline.inputBuffer.id).close(accumulator)
  }

  override def closeMorselAndAccumulatorTask(pipeline: ExecutablePipeline,
                                             inputMorsel: MorselExecutionContext,
                                             accumulator: MorselAccumulator[_]): Unit = {
    closeWorkUnit(pipeline)
    val buffer = buffers.lhsAccumulatingRhsStreamingBuffer(pipeline.inputBuffer.id)
    buffer.close(accumulator, inputMorsel)
  }

  override def filterCancelledArguments(pipeline: ExecutablePipeline,
                                        inputMorsel: MorselExecutionContext): Boolean = {
    val isCancelled = buffers.morselBuffer(pipeline.inputBuffer.id).filterCancelledArguments(inputMorsel)
    if (isCancelled)
      closeWorkUnit(pipeline)
    isCancelled
  }

  override def filterCancelledArguments(pipeline: ExecutablePipeline,
                                        accumulator: MorselAccumulator[_]): Boolean = {
    val isCancelled = buffers.argumentStateBuffer(pipeline.inputBuffer.id).filterCancelledArguments(accumulator)
    if (isCancelled)
      closeWorkUnit(pipeline)
    isCancelled
  }

  override def filterCancelledArguments(pipeline: ExecutablePipeline,
                                                                  inputMorsel: MorselExecutionContext,
                                                                  accumulator: MorselAccumulator[_]): Boolean = {
    val buffer = buffers.lhsAccumulatingRhsStreamingBuffer(pipeline.inputBuffer.id)
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
    val argumentSlotOffset = stateDefinition.argumentStateMaps(argumentStateMapId.x).argumentSlotOffset
    val asm = stateFactory.newArgumentStateMap(argumentStateMapId, argumentSlotOffset, factory)
    argumentStateMaps(argumentStateMapId.x) = asm
    asm
  }

  override def failQuery(throwable: Throwable): Unit = tracker.error(throwable)

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
