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
      pipelineTreeBuilder.applyRhsPlans.toMap
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
    bufferDefinition match {
      case b: ApplyBufferDefinitionBuild =>
        ApplyBufferDefinition(b.id, b.argumentSlotOffset, b.reducers, b.workCancellers.map(mapArgumentStateDefinition), b.reducersOnRHS.map(mapArgumentStateDefinition), b.delegates)
      case b: ArgumentStateBufferDefinitionBuild =>
        ArgumentStateBufferDefinition(b.id, b.argumentStateMapId, b.reducers, b.workCancellers.map(mapArgumentStateDefinition))
      case b: MorselBufferDefinitionBuild =>
        MorselBufferDefinition(b.id, b.reducers, b.workCancellers.map(mapArgumentStateDefinition))
      case b: DelegateBufferDefinitionBuild =>
        MorselBufferDefinition(b.id, b.reducers, b.workCancellers.map(mapArgumentStateDefinition))
      case b: LHSAccumulatingRHSStreamingBufferDefinitionBuild =>
        LHSAccumulatingRHSStreamingBufferDefinition(b.id, b.lhsPipelineId, b.rhsPipelineId, b.lhsArgumentStateMapId, b.rhsArgumentStateMapId, b.reducers, b.workCancellers.map(mapArgumentStateDefinition))
      case _ =>
        throw new IllegalStateException(s"Unexpected type of BufferDefinitionBuild: $bufferDefinition")
    }
  }

  private def mapArgumentStateDefinition(build: ArgumentStateDefinitionBuild): ArgumentStateDefinition =
    ArgumentStateDefinition(build.id, build.planId, build.argumentSlotOffset)

}
