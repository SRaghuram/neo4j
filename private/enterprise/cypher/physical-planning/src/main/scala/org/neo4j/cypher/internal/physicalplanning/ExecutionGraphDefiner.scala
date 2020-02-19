/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ExecutionStateDefiner

object ExecutionGraphDefiner {

  /**
   * Builds an [[ExecutionGraphDefinition]], including [[PipelineDefinition]]s, for a given physical plan.
   */
  def defineFrom(breakingPolicy: PipelineBreakingPolicy,
                 operatorFusionPolicy: OperatorFusionPolicy,
                 physicalPlan: PhysicalPlan): ExecutionGraphDefinition = {

    val executionStateDefiner = new ExecutionStateDefiner(physicalPlan)
    val pipelineTreeBuilder = new PipelineTreeBuilder(breakingPolicy, operatorFusionPolicy, executionStateDefiner, physicalPlan.slotConfigurations, physicalPlan.argumentSizes)

    pipelineTreeBuilder.build(physicalPlan.logicalPlan)
    ExecutionGraphDefinition(physicalPlan,
      executionStateDefiner.buffers.map(_.result),
      executionStateDefiner.argumentStateMaps,
      pipelineTreeBuilder.pipelines.map(_.result),
      pipelineTreeBuilder.applyRhsPlans.toMap)
  }
}