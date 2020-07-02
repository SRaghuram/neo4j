/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Per query unique buffer id
 */
case class BufferId(x: Int) extends AnyVal

/**
 * Per query unique pipeline id
 */
case class PipelineId(x: Int) extends AnyVal

/**
 * per query unique argument state map id
 */
case class ArgumentStateMapId(x: Int) extends AnyVal

object PipelineId {
  val NO_PIPELINE: PipelineId = PipelineId(-1)
}

/**
 * Maps to one ExecutablePipeline
 *
 * @param id               The ID of the pipeline
 * @param lhs              The ID of the LHS pipeline of this pipeline, or NO_PIPELINE
 * @param rhs              The ID of the RHS pipeline of this pipeline, or NO_PIPELINE
 * @param headPlan         The first plan of the pipeline
 * @param inputBuffer      The input buffer to the pipeline
 * @param outputDefinition The output of the pipeline
 * @param middlePlans      Contains all non-fused middle plans of the pipeline
 * @param serial           `true` if the pipeline should be executed serially otherwise `false`
 * @param workLimiter      Optional reference to a workCanceller via an ArgumentStateMapId
 */
case class PipelineDefinition(id: PipelineId,
                              lhs: PipelineId,
                              rhs: PipelineId,
                              headPlan: HeadPlan,
                              inputBuffer: BufferDefinition,
                              outputDefinition: OutputDefinition,
                              middlePlans: IndexedSeq[LogicalPlan],
                              serial: Boolean,
                              workLimiter: Option[ArgumentStateMapId])

/**
 * Maps to one ArgumentStateMap.
 */
case class ArgumentStateDefinition(id: ArgumentStateMapId,
                                   planId: Id,
                                   argumentSlotOffset: Int)


// -- BUFFERS

/**
 * A buffer between two pipelines, or a delegate after an ApplyBuffer. Maps to a PipelinedBuffer.
 *
 * @param memoryTrackingOperatorId the operator that consumes from this buffer. This information is needed for correct memory tracking per operator
 */
case class BufferDefinition(id: BufferId,
                            memoryTrackingOperatorId: Id,
                            // We need multiple reducers because a buffer might need to
                            // reference count for multiple downstream reduce operators,
                            // at potentially different argument depths
                            reducers: ReadOnlyArray[ArgumentStateMapId],
                            workCancellers: ReadOnlyArray[Initialization[ArgumentStateMapId]],
                            variant: BufferVariant)(val bufferSlotConfiguration: SlotConfiguration) {
  val workCancellerIds: ReadOnlyArray[ArgumentStateMapId] = workCancellers.map(_.receiver)

  def withReducers(reducers: ReadOnlyArray[ArgumentStateMapId]): BufferDefinition =
    copy(reducers = reducers)(bufferSlotConfiguration)

  def withWorkCancellers(workCancellers: ReadOnlyArray[Initialization[ArgumentStateMapId]]): BufferDefinition =
    copy(workCancellers = workCancellers)(bufferSlotConfiguration)
}

/**
 * Common superclass of all buffer variants.
 */
sealed trait BufferVariant

/**
 * Regular pipelined buffer.
 */
case object RegularBufferVariant extends BufferVariant

/**
 * A buffer between two pipelines before an Optional/Anti operator, or a delegate after an ApplyBuffer. Maps to an ArgumentStreamMorselBuffer/AntiMorselBuffer.
 */
case class ArgumentStreamBufferVariant(argumentStateMapId: ArgumentStateMapId,
                                       bufferVariantType: ArgumentStreamBufferVariantType) extends BufferVariant

sealed trait ArgumentStreamBufferVariantType
case object ArgumentStreamType extends ArgumentStreamBufferVariantType
case object AntiType extends ArgumentStreamBufferVariantType

/**
 * The information representing the initialization of a receiver with a certain initial count.
 */
case class Initialization[T](receiver: T, initialCount: Int)

/**
 * Sits between the LHS and RHS of an apply.
 * This acts as a multiplexer. It receives input and copies it into
 *
 * @param reducersOnRHSReversed Initializations of ArgumentStates of reducers on the RHS of this Apply, in downstream -> upstream order.
 *                              This order is convenient since upstream reducers possibly need to increment counts on
 *                              their downstreams, which have to be initialized first in order to do that.
 */
case class ApplyBufferVariant(argumentSlotOffset: Int,
                              reducersOnRHSReversed: ReadOnlyArray[Initialization[ArgumentStateMapId]],
                              downstreamStatesOnRHS: ReadOnlyArray[ArgumentStateMapId],
                              delegates: ReadOnlyArray[BufferId]) extends BufferVariant

case class AttachBufferVariant(applyBuffer: BufferDefinition,
                               outputSlots: SlotConfiguration,
                               argumentSlotOffset: Int,
                               argumentSize: SlotConfiguration.Size) extends BufferVariant

case class ConditionalBufferVariant(onTrue: BufferDefinition, onFalse: BufferDefinition, expression: Expression) extends BufferVariant

/**
 * This buffer groups data by argument row and sits between a pre-reduce and a reduce operator.
 * Maps to a PipelinedArgumentStateBuffer.
 */
case class ArgumentStateBufferVariant(argumentStateMapId: ArgumentStateMapId) extends BufferVariant

/**
 * This buffer maps to a LHSAccumulatingSink. It sits after a hash join build.
 */
case class LHSAccumulatingBufferVariant(lhsArgumentStateMapId: ArgumentStateMapId, rhsArgumentStateMapId: ArgumentStateMapId) extends BufferVariant
/**
 * This buffer maps to a RHSStreamingSink. It sits after a hash join 'pre-probe' (buffering data for later probe).
 */
case class RHSStreamingBufferVariant(lhsArgumentStateMapId: ArgumentStateMapId, rhsArgumentStateMapId: ArgumentStateMapId) extends BufferVariant
/**
  * This buffer maps to a LHSAccumulatingRHSStreamingSource. It sits before a hash join probe.
 */
case class LHSAccumulatingRHSStreamingBufferVariant(lhsSink: BufferDefinition,
                                                    rhsSink: BufferDefinition,
                                                    lhsArgumentStateMapId: ArgumentStateMapId,
                                                    rhsArgumentStateMapId: ArgumentStateMapId) extends BufferVariant

// -- OUTPUT
sealed trait OutputDefinition
case class ProduceResultOutput(plan: ProduceResult) extends OutputDefinition
case class MorselBufferOutput(id: BufferId, nextPipelineHeadPlanId: Id) extends OutputDefinition
case class MorselArgumentStateBufferOutput(id: BufferId, argumentSlotOffset: Int, nextPipelineHeadPlanId: Id) extends OutputDefinition
case class ReduceOutput(bufferId: BufferId, argumentStateMapId: ArgumentStateMapId, plan: LogicalPlan) extends OutputDefinition
case object NoOutput extends OutputDefinition

// -- EXECUTION GRAPH
case class ExecutionGraphDefinition(physicalPlan: PhysicalPlan,
                                    buffers: ReadOnlyArray[BufferDefinition],
                                    argumentStateMaps: ReadOnlyArray[ArgumentStateDefinition],
                                    pipelines: IndexedSeq[PipelineDefinition],
                                    applyRhsPlans: Map[Int, Int]) {
  def findArgumentStateMapForPlan(planId: Id): ArgumentStateMapId = {
    var i = 0
    while (i < argumentStateMaps.length) {
      val asm = argumentStateMaps(i)
      if (asm.planId == planId)
        return asm.id
      i += 1
    }
    throw new IllegalStateException("Requested an ArgumentStateMap for an operator which does not have any.")
  }
}

object ExecutionGraphDefinition {
  val NO_ARGUMENT_STATE_MAPS: ReadOnlyArray[ArgumentStateMapId] = ReadOnlyArray.empty
  val NO_ARGUMENT_STATE_MAP_INITIALIZATIONS: ReadOnlyArray[Initialization[ArgumentStateMapId]] = ReadOnlyArray.empty
  val NO_BUFFERS: ReadOnlyArray[BufferId] = ReadOnlyArray.empty
}
