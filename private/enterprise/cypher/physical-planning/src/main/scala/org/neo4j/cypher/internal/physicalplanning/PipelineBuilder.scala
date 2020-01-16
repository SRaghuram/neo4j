/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ApplyBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStateBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStateDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.AttachBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.BufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DelegateBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamReduce
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamState
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamWorkCanceller
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ExecutionStateDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.LHSAccumulatingBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.LHSAccumulatingRHSStreamingBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.RHSStreamingBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.MorselBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.OptionalMorselBufferDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.PipelineDefinitionBuild

object PipelineBuilder {

  /**
   * Builds an [[ExecutionGraphDefinition]], including [[PipelineDefinition]]s, for a given physical plan.
   */
  def build(breakingPolicy: PipelineBreakingPolicy,
            operatorFusionPolicy: OperatorFusionPolicy,
            physicalPlan: PhysicalPlan): ExecutionGraphDefinition = {

    val executionStateDefinitionBuild = new ExecutionStateDefinitionBuild(physicalPlan)
    val pipelineTreeBuilder = new PipelineTreeBuilder(breakingPolicy, operatorFusionPolicy, executionStateDefinitionBuild, physicalPlan.slotConfigurations, physicalPlan.argumentSizes)

    pipelineTreeBuilder.build(physicalPlan.logicalPlan)
    ExecutionGraphDefinition(physicalPlan,
      executionStateDefinitionBuild.buffers.map(mapBuffer),
      executionStateDefinitionBuild.argumentStateMaps.map(mapArgumentStateDefinition),
      pipelineTreeBuilder.pipelines.map(mapPipeline),
      pipelineTreeBuilder.applyRhsPlans.toMap)
  }

  private def mapPipeline(pipeline: PipelineDefinitionBuild): PipelineDefinition = {
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

  private def mapBuffer(bufferDefinition: BufferDefinitionBuild): BufferDefinition = {
    val downstreamReducers = bufferDefinition.downstreamStates.collect { case d: DownstreamReduce => d.id }
    val workCancellerIDs = bufferDefinition.downstreamStates.collect { case d: DownstreamWorkCanceller => d.id }
    val downstreamStateIDs = bufferDefinition.downstreamStates.collect { case d: DownstreamState => d.id }

    val variant = bufferDefinition match {
      case b: AttachBufferDefinitionBuild =>
        AttachBufferVariant(mapBuffer(b.applyBuffer), b.outputSlotConfiguration, b.argumentSlotOffset, b.argumentSize)

      case b: ApplyBufferDefinitionBuild =>
        ApplyBufferVariant(b.argumentSlotOffset,
          b.reducersOnRHS.map(argStateBuild => argStateBuild.id).reverse.toArray,
          b.delegates.toArray)

      case b: ArgumentStateBufferDefinitionBuild =>
        ArgumentStateBufferVariant(b.argumentStateMapId)

      case b: MorselBufferDefinitionBuild =>
        RegularBufferVariant

      case b: OptionalMorselBufferDefinitionBuild =>
        OptionalBufferVariant(b.argumentStateMapId)

      case b: DelegateBufferDefinitionBuild =>
        RegularBufferVariant

      case b: LHSAccumulatingBufferDefinitionBuild =>
        LHSAccumulatingBufferVariant(b.id,
                                     b.argumentStateMapId)

      case b: RHSStreamingBufferDefinitionBuild =>
        RHSStreamingBufferVariant(b.id,
                                  b.argumentStateMapId)

      case b: LHSAccumulatingRHSStreamingBufferDefinitionBuild =>
        LHSAccumulatingRHSStreamingBufferVariant(mapBuffer(b.lhsSink),
                                                 mapBuffer(b.rhsSink),
                                                 b.lhsArgumentStateMapId,
                                                 b.rhsArgumentStateMapId)

      case _ =>
        throw new IllegalStateException(s"Unexpected type of BufferDefinitionBuild: $bufferDefinition")
    }
    BufferDefinition(bufferDefinition.id,
      downstreamReducers.toArray,
      workCancellerIDs.toArray,
      downstreamStateIDs.toArray,
      variant
    )(bufferDefinition.bufferConfiguration)
  }

  private def mapArgumentStateDefinition(build: ArgumentStateDefinitionBuild): ArgumentStateDefinition =
    ArgumentStateDefinition(build.id, build.planId, build.argumentSlotOffset)

}
