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

/**
  * Different types of operators that keep argument state and are interesting to the current (upstream) operator
  * because of different reasons.
  */
sealed trait DownstreamStateOperator

/**
  * An operator needs to count tasks towards a downstream reducer.
  */
case class DownstreamReduce(id: ArgumentStateMapId) extends DownstreamStateOperator

/**
  * An operator needs to check if work has been cancelled with a downstream work canceller.
  */
case class DownstreamWorkCanceller(id: ArgumentStateMapId) extends DownstreamStateOperator

/**
  * An operator needs to initiate argument states for other downstream state operators.
  */
case class DownstreamState(id: ArgumentStateMapId) extends DownstreamStateOperator


// -- BUFFERS

/**
  * Common superclass of all buffer definitions.
  */
sealed trait BufferDefinition {
  def id: BufferId

  def downstreamStates: IndexedSeq[DownstreamStateOperator]

  def withDownstreamStates(downstreamStates: IndexedSeq[DownstreamStateOperator]): BufferDefinition

}

/**
  * A buffer between two pipelines, or a delegate after an ApplyBuffer. Maps to a MorselBuffer.
  */
case class MorselBufferDefinition(id: BufferId,
                                  downstreamStates: IndexedSeq[DownstreamStateOperator])
  extends BufferDefinition {

  override def withDownstreamStates(downstreamStates: IndexedSeq[DownstreamStateOperator]): MorselBufferDefinition = copy(downstreamStates = downstreamStates)
}

/**
  * Sits between the LHS and RHS of an apply.
  * This acts as a multiplexer. It receives input and copies it into
  *
  *
  * @param reducersOnRHS these are ArgumentStates of reducers on the RHS
  */
case class ApplyBufferDefinition(id: BufferId,
                                 argumentSlotOffset: Int,
                                 downstreamStates: IndexedSeq[DownstreamStateOperator],
                                 reducersOnRHS: IndexedSeq[ArgumentStateDefinition],
                                 delegates: IndexedSeq[BufferId])
  extends BufferDefinition {

  override def withDownstreamStates(downstreamStates: IndexedSeq[DownstreamStateOperator]): ApplyBufferDefinition = copy(downstreamStates = downstreamStates)
}

/**
  * This buffer groups data by argument row and sits between a pre-reduce and a reduce operator.
  * Maps to a MorselArgumentStateBuffer.
  */
case class ArgumentStateBufferDefinition(id: BufferId,
                                         argumentStateMapId: ArgumentStateMapId,
                                         downstreamStates: IndexedSeq[DownstreamStateOperator])
  extends BufferDefinition {

  override def withDownstreamStates(downstreamStates: IndexedSeq[DownstreamStateOperator]): ArgumentStateBufferDefinition = copy(downstreamStates = downstreamStates)
}

/**
  * This buffer maps to a LHSAccumulatingRHSStreamingBuffer. It sits before a hash join.
  */
case class LHSAccumulatingRHSStreamingBufferDefinition(id: BufferId,
                                                       lhsPipelineId: PipelineId,
                                                       rhsPipelineId: PipelineId,
                                                       lhsArgumentStateMapId: ArgumentStateMapId,
                                                       rhsArgumentStateMapId: ArgumentStateMapId,
                                                       downstreamStates: IndexedSeq[DownstreamStateOperator])
  extends BufferDefinition {

  override def withDownstreamStates(downstreamStates: IndexedSeq[DownstreamStateOperator]): LHSAccumulatingRHSStreamingBufferDefinition = copy(downstreamStates = downstreamStates)
}

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
                                    applyRhsPlans: Map[Int, Int]) {
  def findArgumentStateMapForPlan(planId: Id): ArgumentStateMapId = {
    argumentStateMaps.find(_.planId == planId).map(_.id).getOrElse {
      throw new IllegalStateException("Requested an ArgumentStateMap for an operator which does not have any.")
    }
  }
}
