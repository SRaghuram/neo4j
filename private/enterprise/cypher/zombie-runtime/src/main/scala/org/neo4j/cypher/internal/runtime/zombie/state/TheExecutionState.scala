/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.util.Preconditions

/**
  * Implementation of [[ExecutionState]].
  */
object TheExecutionState {
  def build(stateDefinition: StateDefinition, executablePipelines: Seq[ExecutablePipeline], stateFactory: StateFactory): ExecutionState =
    new TheExecutionState(stateDefinition.rowBuffers, executablePipelines, stateDefinition.physicalPlan, stateFactory)
}

class TheExecutionState(bufferDefinitions: Seq[RowBufferDefinition],
                        pipelines: Seq[ExecutablePipeline],
                        physicalPlan: PhysicalPlan,
                        stateFactory: StateFactory) extends ExecutionState {

  private val tracker = stateFactory.newTracker()
  private val asms = new ArgumentStateMaps

  // Verify that IDs and offsets match

  for (i <- pipelines.indices)
    Preconditions.checkState(i == pipelines(i).id.x, "Pipeline id does not match offset!")

  for (i <- bufferDefinitions.indices)
    Preconditions.checkState(i == bufferDefinitions(i).id.x, "Buffer definition id does not match offset!")

  // Build state

  private val pipelineStates =
    for (pipeline <- pipelines.toArray) yield {
      pipeline.createState(this)
    }

  private val pipelineLocks =
    for (pipeline <- pipelines.toArray) yield {
      stateFactory.newLock(s"Pipeline[${pipeline.id.x}]")
    }

  private val buffers: Array[MorselBuffer] =
    for (bufferDefinition <- bufferDefinitions.toArray) yield {
      val counters = bufferDefinition.counters.map(_.reducingPlanId)
      bufferDefinition match {
        case x: ArgumentBufferDefinition =>
          new MorselArgumentBuffer(x.argumentSlotOffset,
                                   tracker,
                                   x.countersForThisBuffer.map(_.reducingPlanId),
                                   counters,
                                   asms,
                                   stateFactory.newBuffer(),
                                   stateFactory.newIdAllocator())

        case _: RowBufferDefinition =>
          new MorselBuffer(tracker, counters, asms, stateFactory.newBuffer())
      }
    }

  private val continuations =
    new Array[Buffer[PipelineTask]](pipelines.size).map(i => stateFactory.newBuffer[PipelineTask]())

  // Methods

  override def produceMorsel(bufferId: BufferId,
                             output: MorselExecutionContext): Unit = buffers(bufferId.x).produce(output)

  override def consumeMorsel(bufferId: BufferId, pipeline: ExecutablePipeline): MorselParallelizer = {
    consumeUnderPotentialLock(pipeline, buffers(bufferId.x))
  }

  def closeWorkUnit(task: PipelineTask): Unit = {
    val pipeline = task.pipeline
    if (pipeline.serial)
      pipelineLocks(pipeline.id.x).unlock()
  }

  override def closeTask(task: PipelineTask): Unit = {
    closeWorkUnit(task)
    buffers(task.pipeline.inputRowBuffer.id.x).close(task.start.inputMorsel)
  }

  override def addContinuation(task: PipelineTask): Unit = {
    closeWorkUnit(task)
    continuations(task.pipeline.id.x).produce(task)
  }

  override def continue(pipeline: ExecutablePipeline): PipelineTask = {
    consumeUnderPotentialLock(pipeline, continuations(pipeline.id.x))
  }

  private def consumeUnderPotentialLock[T <: AnyRef](pipeline: ExecutablePipeline, consumable: Consumable[T]): T = {
    if (pipeline.serial) {
      if (consumable.hasData && pipelineLocks(pipeline.id.x).tryLock()) {
        val t = consumable.consume() // the data might have been consumed while we took the lock
        if (t == null)
          pipelineLocks(pipeline.id.x).unlock()
        t
      } else null.asInstanceOf[T]
    } else
      consumable.consume()
  }

  override def initialize(): Unit = {
    buffers.head.produce(MorselExecutionContext.createInitialRow())
  }

  override def pipelineState(pipelineId: PipelineId): PipelineState = pipelineStates(pipelineId.x)

  override final def createArgumentStateMap[T <: MorselAccumulator](reducePlanId: Id,
                                                                    constructor: () => T): ArgumentStateMap[T] = {
    val argumentSlotOffset =
      physicalPlan
        .slotConfigurations(reducePlanId)
        .getArgumentLongOffsetFor(physicalPlan.applyPlans(reducePlanId))

    val asm = stateFactory.newArgumentStateMap(reducePlanId, argumentSlotOffset, constructor)
    asms.set(reducePlanId, asm)
    asm
  }

  override def awaitCompletion(): Unit = tracker.await()

  override def isCompleted: Boolean = tracker.isCompleted
}
