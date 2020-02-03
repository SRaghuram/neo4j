/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ApplyBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStateDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.AttachBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.BufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DelegateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamReduce
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamState
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamWorkCanceller
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ExecutionStateDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.LHSAccumulatingBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.RHSStreamingBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.LHSAccumulatingRHSStreamingBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.MorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.OptionalMorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.PipelineDefiner

object PipelineBuilder {

  /**
   * Builds an [[ExecutionGraphDefinition]], including [[PipelineDefinition]]s, for a given physical plan.
   */
  def build(breakingPolicy: PipelineBreakingPolicy,
            operatorFusionPolicy: OperatorFusionPolicy,
            physicalPlan: PhysicalPlan): ExecutionGraphDefinition = {

    val executionStateDefiner = new ExecutionStateDefiner(physicalPlan)
    val pipelineTreeBuilder = new PipelineTreeBuilder(breakingPolicy, operatorFusionPolicy, executionStateDefiner, physicalPlan.slotConfigurations, physicalPlan.argumentSizes)

    pipelineTreeBuilder.build(physicalPlan.logicalPlan)
    ExecutionGraphDefinition(physicalPlan,
      executionStateDefiner.buffers.map(mapBuffer),
      executionStateDefiner.argumentStateMaps.map(mapArgumentStateDefinition),
      pipelineTreeBuilder.pipelines.map(mapPipeline),
      pipelineTreeBuilder.applyRhsPlans.toMap)
  }

  private def mapPipeline(pipeline: PipelineDefiner): PipelineDefinition = {
    PipelineDefinition(
      pipeline.id,
      pipeline.lhs,
      pipeline.rhs,
      pipeline.headPlan,
      pipeline.fusedPlans,
      mapBuffer(pipeline.inputBuffer),
      pipeline.outputDefinition,
      pipeline.middlePlans,
      pipeline.serial)
  }

  private def mapBuffer(bufferDefiner: BufferDefiner): BufferDefinition = {
    val downstreamReducers = bufferDefiner.downstreamStates.collect { case d: DownstreamReduce => d.id }
    val workCancellerIDs = bufferDefiner.downstreamStates.collect { case d: DownstreamWorkCanceller => d.id }
    val downstreamStateIDs = bufferDefiner.downstreamStates.collect { case d: DownstreamState => d.id }

    val variant = bufferDefiner match {
      case b: AttachBufferDefiner =>
        AttachBufferVariant(mapBuffer(b.applyBuffer), b.outputSlotConfiguration, b.argumentSlotOffset, b.argumentSize)

      case b: ApplyBufferDefiner =>
        ApplyBufferVariant(b.argumentSlotOffset,
          b.reducersOnRHS.map(argStateBuild => argStateBuild.id).reverse.toArray,
          b.delegates.toArray)

      case b: ArgumentStateBufferDefiner =>
        ArgumentStateBufferVariant(b.argumentStateMapId)

      case _: MorselBufferDefiner =>
        RegularBufferVariant

      case b: OptionalMorselBufferDefiner =>
        OptionalBufferVariant(b.argumentStateMapId)

      case _: DelegateBufferDefiner =>
        RegularBufferVariant

      case b: LHSAccumulatingBufferDefiner =>
        LHSAccumulatingBufferVariant(b.id,
                                     b.argumentStateMapId)

      case b: RHSStreamingBufferDefiner =>
        RHSStreamingBufferVariant(b.id,
                                  b.argumentStateMapId)

      case b: LHSAccumulatingRHSStreamingBufferDefiner =>
        LHSAccumulatingRHSStreamingBufferVariant(mapBuffer(b.lhsSink),
                                                 mapBuffer(b.rhsSink),
                                                 b.lhsArgumentStateMapId,
                                                 b.rhsArgumentStateMapId)

      case _ =>
        throw new IllegalStateException(s"Unexpected type of BufferDefiner: $bufferDefiner")
    }
    BufferDefinition(bufferDefiner.id,
      bufferDefiner.operatorId,
      downstreamReducers.toArray,
      workCancellerIDs.toArray,
      downstreamStateIDs.toArray,
      variant
    )(bufferDefiner.bufferConfiguration)
  }

  private def mapArgumentStateDefinition(build: ArgumentStateDefiner): ArgumentStateDefinition =
    ArgumentStateDefinition(build.id, build.planId, build.argumentSlotOffset)

}
