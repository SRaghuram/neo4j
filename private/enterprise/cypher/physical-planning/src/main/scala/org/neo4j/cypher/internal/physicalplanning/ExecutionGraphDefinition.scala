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
  */
case class PipelineDefinition(id: PipelineId,
                              headPlan: LogicalPlan,
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
                            reducers: IndexedSeq[ArgumentStateMapId],
                            workCancellers: IndexedSeq[ArgumentStateMapId],
                            downstreamStates: IndexedSeq[ArgumentStateMapId],
                            variant: BufferVariant) {
  def withReducers(reducers: IndexedSeq[ArgumentStateMapId]): BufferDefinition = copy(reducers = reducers)

  def withWorkCancellers(workCancellers: IndexedSeq[ArgumentStateMapId]): BufferDefinition = copy(workCancellers = workCancellers)
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
  *
  * @param reducersOnRHS these are ArgumentStates of reducers on the RHS
  */
case class ApplyBufferVariant(argumentSlotOffset: Int,
                              reducersOnRHS: IndexedSeq[ArgumentStateDefinition],
                              delegates: IndexedSeq[BufferId]) extends BufferVariant

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
case class MorselBufferOutput(id: BufferId) extends OutputDefinition
case class MorselArgumentStateBufferOutput(id: BufferId, argumentSlotOffset: Int) extends OutputDefinition
case class ReduceOutput(bufferId: BufferId, plan: LogicalPlan) extends OutputDefinition
case object NoOutput extends OutputDefinition

// -- EXECUTION GRAPH
case class ExecutionGraphDefinition(physicalPlan: PhysicalPlan,
                                    buffers: IndexedSeq[BufferDefinition],
                                    argumentStateMaps: IndexedSeq[ArgumentStateDefinition],
                                    pipelines: IndexedSeq[PipelineDefinition],
                                    applyRhsPlans: Map[Int, Int],
                                    transactionMaxMemory: Long) {
  def findArgumentStateMapForPlan(planId: Id): ArgumentStateMapId = {
    argumentStateMaps.find(_.planId == planId).map(_.id).getOrElse {
      throw new IllegalStateException("Requested an ArgumentStateMap for an operator which does not have any.")
    }
  }
}
