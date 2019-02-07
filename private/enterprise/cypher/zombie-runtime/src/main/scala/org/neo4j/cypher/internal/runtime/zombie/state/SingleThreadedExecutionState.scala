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

import scala.collection.mutable.ArrayBuffer

object SingleThreadedStateBuilder {

  def build(stateDefinition: StateDefinition,
            executablePipelines: IndexedSeq[ExecutablePipeline]): ExecutionState =
    new SingleThreadedExecutionState(stateDefinition.rowBuffers,
                                     executablePipelines,
                                     stateDefinition.physicalPlan)
}

/**
  * Not thread-safe implementation of [[ExecutionState]].
  */
class SingleThreadedExecutionState(bufferDefinitions: Seq[RowBufferDefinition],
                                   pipelines: Seq[ExecutablePipeline],
                                   physicalPlan: PhysicalPlan) extends ExecutionState {

  private val asms = new ArgumentStateMaps

  private val pipelineStates =
    for (pipeline <- pipelines.toArray) yield {
      pipeline.createState(this)
    }

  for (i <- pipelineStates.indices)
    Preconditions.checkState(i == pipelineStates(i).pipeline.id.x, "Pipeline id does not match offset!")

  private val buffers: Array[MorselBuffer] =
    for (bufferDefinition <- bufferDefinitions.toArray) yield {
      val counters = bufferDefinition.counters.map(_.reducingPlanId)
      bufferDefinition match {
        case x: ArgumentBufferDefinition =>
          new ArgumentBuffer(x.argumentSlotOffset, counters, asms)
        case _: RowBufferDefinition =>
          new MorselBuffer(counters, asms)
      }
    }

  for (i <- bufferDefinitions.indices)
    Preconditions.checkState(i == bufferDefinitions(i).id.x, "Buffer definition id does not match offset!")

  private val continuations = new Array[RegularBuffer[PipelineTask]](pipelines.size).map(i => new RegularBuffer[PipelineTask]())

  override def produceMorsel(bufferId: BufferId,
                             output: MorselExecutionContext): Unit = buffers(bufferId.x).produce(output)

  override def consumeMorsel(bufferId: BufferId): MorselExecutionContext = buffers(bufferId.x).consume()

  override def closeMorsel(bufferId: BufferId,
                           inputMorsel: MorselExecutionContext): Unit = {
    buffers(bufferId.x).close(inputMorsel)
  }

  override def addContinuation(task: PipelineTask): Unit = continuations(task.pipeline.id.x).produce(task)

  override def continue(p: ExecutablePipeline): PipelineTask = continuations(p.id.x).consume()

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

    val asm = new SingleThreadedArgumentStateMap(reducePlanId, argumentSlotOffset, constructor)
    asms.set(reducePlanId, asm)
    asm
  }
}

/**
  * Basic buffer.
  */
trait Buffer[T <: AnyRef] {
  def produce(t: T): Unit
  def consume(): T
}

/**
  * Implementation of a standard non-Thread-safe buffer of elements of type T.
  */
class RegularBuffer[T <: AnyRef] extends Buffer[T] {
  private val data = new ArrayBuffer[T]
  override def produce(t: T): Unit = {
    data.append(t)
  }
  override def consume(): T = {
    if (data.isEmpty) null.asInstanceOf[T]
    else data.remove(0)
  }
}

/**
  * Morsel buffer which adds reference counting of arguments to the regular buffer semantics.
  *
  * @param counters Ids of downstream logical plans which need to reference count the morsels in this buffer.
  * @param argumentStateMaps the ArgumentStateMap attribute for all logical plans
  */
class MorselBuffer(counters: Seq[Id],
                   argumentStateMaps: ArgumentStateMaps
                  ) extends Buffer[MorselExecutionContext] {
  private val data = new ArrayBuffer[MorselExecutionContext]

  /**
    * Decrement reference counters attached to `morsel`.
    */
  def close(morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- morsel.getAndClearCounters()
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.decrement(argumentId)
  }

  override def produce(morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- counters
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.increment(argumentId)
    morsel.resetToFirstRow()
    morsel.setCounters(counters)
    data.append(morsel)
  }

  override def consume(): MorselExecutionContext = {
    if (data.isEmpty) null
    else data.remove(0)
  }
}

/**
  * Extension of [[MorselBuffer]], which generates and writes argument row ids into given argumentSlotOffset.
  *
  * @param argumentSlotOffset slot to which argument row ids are written.
  */
class ArgumentBuffer(argumentSlotOffset: Int,
                     counters: Seq[Id],
                     argumentStateMaps: ArgumentStateMaps
                    ) extends MorselBuffer(counters, argumentStateMaps) {
  private var argumentRowCount = 0L

  override def produce(morsel: MorselExecutionContext): Unit = {
    morsel.resetToFirstRow()
    while (morsel.isValidRow) {
      morsel.setLongAt(argumentSlotOffset, argumentRowCount)
      argumentRowCount += 1
      morsel.moveToNextRow()
    }
    super.produce(morsel)
  }
}
