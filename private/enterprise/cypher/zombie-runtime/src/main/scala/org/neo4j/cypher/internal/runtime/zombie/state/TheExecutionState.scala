/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.runtime.zombie.execution.WorkerWaker
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.util.Preconditions

/**
  * Implementation of [[ExecutionState]].
  */
object TheExecutionState {
  def build(stateDefinition: StateDefinition, executablePipelines: Seq[ExecutablePipeline], stateFactory: StateFactory, workerWaker: WorkerWaker): ExecutionState =
    new TheExecutionState(stateDefinition.buffers, executablePipelines, stateDefinition.physicalPlan, stateFactory, workerWaker)
}

class TheExecutionState(bufferDefinitions: Seq[BufferDefinition],
                        pipelines: Seq[ExecutablePipeline],
                        physicalPlan: PhysicalPlan,
                        stateFactory: StateFactory,
                        workerWaker: WorkerWaker) extends ExecutionState {

  private val tracker = stateFactory.newTracker()
  private val argumentStateMaps = new ArgumentStateMaps

  // Verify that IDs and offsets match

  for (i <- pipelines.indices)
    Preconditions.checkState(i == pipelines(i).id.x, "Pipeline id does not match offset!")

  for (i <- bufferDefinitions.indices)
    Preconditions.checkState(i == bufferDefinitions(i).id.x, "Buffer definition id does not match offset!")

  // State

  private var pipelineLocks: Array[Lock] = _
  private var buffers: Buffers = _
  private var continuations: Array[Buffer[PipelineTask]] = _

  override def initializeState(): Unit = {
    pipelineLocks =
      for (pipeline <- pipelines.toArray) yield {
        stateFactory.newLock(s"Pipeline[${pipeline.id.x}]")
      }

    buffers = new Buffers(bufferDefinitions, tracker, argumentStateMaps, stateFactory)
    continuations = new Array[Int](pipelines.size).map(_ => stateFactory.newBuffer[PipelineTask]())

    putMorsel(BufferId(0), MorselExecutionContext.createInitialRow())
  }

  // Methods

  override def putMorsel(bufferId: BufferId,
                         output: MorselExecutionContext): Unit = {
    buffers.sink(bufferId).put(output)
    workerWaker.wakeAll()
  }

  override def takeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer = {
    takeUnderPotentialLock(pipeline, buffers.morselBuffer(bufferId))
  }

  override def takeAccumulators[ACC <: MorselAccumulator](bufferId: BufferId, pipeline: ExecutablePipeline): Iterable[ACC] = {
    takeUnderPotentialLock(pipeline, buffers.argumentStateMapBuffer(bufferId))
  }

  private def closeWorkUnit(pipeline: ExecutablePipeline): Unit = {
    if (pipeline.serial)
      pipelineLocks(pipeline.id.x).unlock()
  }

  override def closeMorselTask(pipeline: ExecutablePipeline, inputMorsel: MorselExecutionContext): Unit = {
    closeWorkUnit(pipeline)
    buffers.morselBuffer(pipeline.inputBuffer.id).close(inputMorsel)
  }

  override def closeAccumulatorsTask[ACC <: MorselAccumulator](pipeline: ExecutablePipeline, accumulators: Iterable[ACC]): Unit = {
    closeWorkUnit(pipeline)
    buffers.argumentStateMapBuffer(pipeline.inputBuffer.id).close(accumulators)
  }

  override def putContinuation(task: PipelineTask): Unit = {
    closeWorkUnit(task.pipelineState.pipeline)
    continuations(task.pipelineState.pipeline.id.x).put(task)
    workerWaker.wakeAll()
  }

  override def continue(pipeline: ExecutablePipeline): PipelineTask = {
    takeUnderPotentialLock(pipeline, continuations(pipeline.id.x))
  }

  private def takeUnderPotentialLock[T <: AnyRef](pipeline: ExecutablePipeline, source: Source[T]): T = {
    if (pipeline.serial) {
      if (source.hasData && pipelineLocks(pipeline.id.x).tryLock()) {
        val t = source.take()
        if (t == null) // the data might have been taken while we took the lock
          pipelineLocks(pipeline.id.x).unlock()
        t
      } else null.asInstanceOf[T]
    } else
      source.take()
  }

  override final def createArgumentStateMap[S <: ArgumentState](reducePlanId: Id,
                                                                factory: ArgumentStateFactory[S]): ArgumentStateMap[S] = {
    val argumentSlotOffset =
      physicalPlan
        .slotConfigurations(reducePlanId)
        .getArgumentLongOffsetFor(physicalPlan.applyPlans(reducePlanId))

    val asm = stateFactory.newArgumentStateMap(reducePlanId, argumentSlotOffset, factory)
    argumentStateMaps.set(reducePlanId, asm)
    asm
  }

  override def awaitCompletion(): Unit = tracker.await()

  override def isCompleted: Boolean = tracker.isCompleted
}
