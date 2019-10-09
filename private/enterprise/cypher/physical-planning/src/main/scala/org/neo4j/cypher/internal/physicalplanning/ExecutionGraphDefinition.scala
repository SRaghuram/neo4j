/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, ProduceResult}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id


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
 * @param id               the id of the pipeline
 * @param lhs              The ID of the LHS pipeline of this pipeline, or NO_PIPELINE
 * @param rhs              The ID of the RHS pipeline of this pipeline, or NO_PIPELINE
 * @param headPlan         the first plan of the pipeline
 * @param fusedPlans       plans that can be fused together, contains head plan (if fuseable) and all consecutively fuseable middlePlans.
 *                         If a plan appears in the list of fuseable plans it will not appear in the list of middlePlans.
 * @param inputBuffer      The input buffer to the pipeline
 * @param outputDefinition The output of the pipeline
 * @param middlePlans      Contains all non-fused middle plans of the pipeline
 * @param serial           `true` if the pipeline should be executed serially otherwise `false`
 */
case class PipelineDefinition(id: PipelineId,
                              lhs: PipelineId,
                              rhs: PipelineId,
                              headPlan: LogicalPlan,
                              fusedPlans: IndexedSeq[LogicalPlan],
                              inputBuffer: BufferDefinition,
                              outputDefinition: OutputDefinition,
                              middlePlans: IndexedSeq[LogicalPlan],
                              serial: Boolean)

/**
  * Maps to one ArgumentStateMap.
  */
case class ArgumentStateDefinition(id: ArgumentStateMapId,
                                   planId: Id,
                                   argumentSlotOffset: Int)


// -- BUFFERS

/**
  * A buffer between two pipelines, or a delegate after an ApplyBuffer. Maps to a MorselBuffer.
  */
case class BufferDefinition(id: BufferId,
                            // We need multiple reducers because a buffer might need to
                            // reference count for multiple downstream reduce operators,
                            // at potentially different argument depths
                            reducers: Array[ArgumentStateMapId],
                            workCancellers: Array[ArgumentStateMapId],
                            downstreamStates: Array[ArgumentStateMapId],
                            variant: BufferVariant)(val bufferSlotConfiguration: SlotConfiguration) {
  def withReducers(reducers: IndexedSeq[ArgumentStateMapId]): BufferDefinition =
    copy(reducers = reducers.toArray)(bufferSlotConfiguration)

  def withWorkCancellers(workCancellers: IndexedSeq[ArgumentStateMapId]): BufferDefinition =
    copy(workCancellers = workCancellers.toArray)(bufferSlotConfiguration)

  // Override equality for correct array handling. Only used in tests so not performant.

  override def canEqual(that: Any): Boolean = that.isInstanceOf[BufferDefinition]

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[BufferDefinition] && {
      val other = obj.asInstanceOf[BufferDefinition]
      asTuple.equals(other.asTuple)
    }
  }

  override def hashCode(): Int = asTuple.hashCode()

  private def asTuple: (BufferId, Seq[ArgumentStateMapId], Seq[ArgumentStateMapId], Seq[ArgumentStateMapId], BufferVariant) =
    (id, reducers, workCancellers, downstreamStates, variant)
}

/**
  * Common superclass of all buffer variants.
  */
sealed trait BufferVariant

/**
  * Regular morsel buffer.
  */
case object RegularBufferVariant extends BufferVariant

/**
  * A buffer between two pipelines before an Optional operator, or a delegate after an ApplyBuffer. Maps to an OptionalMorselBuffer.
  */
case class OptionalBufferVariant(argumentStateMapId: ArgumentStateMapId) extends BufferVariant

/**
  * Sits between the LHS and RHS of an apply.
  * This acts as a multiplexer. It receives input and copies it into
  *
  * @param reducersOnRHSReversed ArgumentStates of reducers on the RHS of this Apply, in downstream -> upstream order.
  *                              This order is convenient since upstream reducers possibly need to increment counts on
  *                              their downstreams, which have to be initialized first in order to do that.
  */
case class ApplyBufferVariant(argumentSlotOffset: Int,
                              reducersOnRHSReversed: Array[ArgumentStateMapId],
                              delegates: Array[BufferId]) extends BufferVariant {

  // Override equality for correct array handling. Only used in tests so not performant.

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ApplyBufferVariant]

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[ApplyBufferVariant] && {
      val other = obj.asInstanceOf[ApplyBufferVariant]
      asTuple.equals(other.asTuple)
    }
  }

  override def hashCode(): Int = asTuple.hashCode()

  private def asTuple: (Int, Seq[ArgumentStateMapId], Seq[BufferId]) =
    (argumentSlotOffset, reducersOnRHSReversed, delegates)
}

case class AttachBufferVariant(applyBuffer: BufferDefinition,
                               outputSlots: SlotConfiguration,
                               argumentSlotOffset: Int,
                               argumentSize: SlotConfiguration.Size) extends BufferVariant

/**
  * This buffer groups data by argument row and sits between a pre-reduce and a reduce operator.
  * Maps to a MorselArgumentStateBuffer.
  */
case class ArgumentStateBufferVariant(argumentStateMapId: ArgumentStateMapId) extends BufferVariant

/**
  * This buffer maps to a LHSAccumulatingRHSStreamingBuffer. It sits before a hash join.
  */
case class LHSAccumulatingRHSStreamingBufferVariant(lhsPipelineId: PipelineId,
                                                    rhsPipelineId: PipelineId,
                                                    lhsArgumentStateMapId: ArgumentStateMapId,
                                                    rhsArgumentStateMapId: ArgumentStateMapId) extends BufferVariant

// -- OUTPUT
sealed trait OutputDefinition
case class ProduceResultOutput(plan: ProduceResult) extends OutputDefinition
case class MorselBufferOutput(id: BufferId, nextPipelineHeadPlanId: Id) extends OutputDefinition
case class MorselArgumentStateBufferOutput(id: BufferId, argumentSlotOffset: Int, nextPipelineHeadPlanId: Id) extends OutputDefinition
case class ReduceOutput(bufferId: BufferId, plan: LogicalPlan) extends OutputDefinition
case object NoOutput extends OutputDefinition

// -- EXECUTION GRAPH
case class ExecutionGraphDefinition(physicalPlan: PhysicalPlan,
                                    buffers: IndexedSeq[BufferDefinition],
                                    argumentStateMaps: IndexedSeq[ArgumentStateDefinition],
                                    pipelines: IndexedSeq[PipelineDefinition],
                                    applyRhsPlans: Map[Int, Int]) {
  def findArgumentStateMapForPlan(planId: Id): ArgumentStateMapId = {
    argumentStateMaps.find(_.planId == planId).map(_.id).getOrElse {
      throw new IllegalStateException("Requested an ArgumentStateMap for an operator which does not have any.")
    }
  }
}

object ExecutionGraphDefinition {
  val NO_ARGUMENT_STATE_MAPS = new Array[ArgumentStateMapId](0)
  val NO_BUFFERS = new Array[BufferId](0)
}
