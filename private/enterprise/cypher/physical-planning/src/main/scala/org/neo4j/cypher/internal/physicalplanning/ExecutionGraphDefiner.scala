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
                 operatorFuserFactory: OperatorFuserFactory,
                 physicalPlan: PhysicalPlan): ExecutionGraphDefinition = {

    val executionStateDefiner = new ExecutionStateDefiner(physicalPlan)
    val pipelineTreeBuilder = new PipelineTreeBuilder(breakingPolicy,
                                                      operatorFuserFactory,
                                                      executionStateDefiner,
                                                      physicalPlan.slotConfigurations,
                                                      physicalPlan.argumentSizes,
                                                      physicalPlan.applyPlans)

    pipelineTreeBuilder.build(physicalPlan.logicalPlan)
    ExecutionGraphDefinition(physicalPlan,
      new ReadOnlyArray(executionStateDefiner.buffers.map(_.result).toArray),
      new ReadOnlyArray(executionStateDefiner.argumentStateMaps.toArray),
      pipelineTreeBuilder.pipelines.map(_.result),
      pipelineTreeBuilder.applyRhsPlans.toMap)
  }
}
