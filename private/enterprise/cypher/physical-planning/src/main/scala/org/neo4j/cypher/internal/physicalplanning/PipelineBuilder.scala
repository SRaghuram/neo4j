/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder._


object PipelineBuilder {

  /**
    * Builds an [[ExecutionGraphDefinition]], including [[PipelineDefinition]]s, for a given physical plan.
    */
  def build(breakingPolicy: PipelineBreakingPolicy,
            physicalPlan: PhysicalPlan): ExecutionGraphDefinition = {

    val executionStateDefinitionBuild = new ExecutionStateDefinitionBuild(physicalPlan)
    val pipelineTreeBuilder = new PipelineTreeBuilder(breakingPolicy, executionStateDefinitionBuild, physicalPlan.slotConfigurations)

    pipelineTreeBuilder.build(physicalPlan.logicalPlan)
    ExecutionGraphDefinition(physicalPlan,
      executionStateDefinitionBuild.buffers.map(mapBuffer),
      executionStateDefinitionBuild.argumentStateMaps.map(mapArgumentStateDefinition),
      pipelineTreeBuilder.pipelines.map(mapPipeline),
      pipelineTreeBuilder.applyRhsPlans.toMap,
      physicalPlan.transactionMaxMemory
    )
  }

  private def mapPipeline(pipeline: PipelineDefinitionBuild): PipelineDefinition = {
    PipelineDefinition(
      pipeline.id,
      pipeline.headPlan,
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
      case b: ApplyBufferDefinitionBuild =>
        ApplyBufferVariant(b.argumentSlotOffset,
                           b.reducersOnRHS.map(mapArgumentStateDefinition),
                           b.delegates)

      case b: ArgumentStateBufferDefinitionBuild =>
        ArgumentStateBufferVariant(b.argumentStateMapId)

      case b: MorselBufferDefinitionBuild =>
        RegularBufferVariant

      case b: OptionalMorselBufferDefinitionBuild =>
        OptionalBufferVariant(b.argumentStateMapId)

      case b: DelegateBufferDefinitionBuild =>
        RegularBufferVariant

      case b: LHSAccumulatingRHSStreamingBufferDefinitionBuild =>
        LHSAccumulatingRHSStreamingBufferVariant(b.lhsPipelineId, b.rhsPipelineId, b.lhsArgumentStateMapId, b.rhsArgumentStateMapId)

      case _ =>
        throw new IllegalStateException(s"Unexpected type of BufferDefinitionBuild: $bufferDefinition")
    }
    BufferDefinition(bufferDefinition.id, downstreamReducers, workCancellerIDs, downstreamStateIDs, variant)
  }

  private def mapArgumentStateDefinition(build: ArgumentStateDefinitionBuild): ArgumentStateDefinition =
    ArgumentStateDefinition(build.id, build.planId, build.argumentSlotOffset)

}
