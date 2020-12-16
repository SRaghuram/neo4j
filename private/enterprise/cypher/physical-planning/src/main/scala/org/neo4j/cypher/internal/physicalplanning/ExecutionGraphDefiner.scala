/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ExecutionStateDefiner
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters

object ExecutionGraphDefiner {

  /**
   * @param pipelineTemplates for each pipeline a sequence of templates that can be compiled to an Operator.
   *                          This is not part of the executionGraphDefinition, since these Templates are lambda functions
   *                          that we only need while building the ExecutableQuery, so we do not want to cache them,
   *                          unlike the rest of the executionGraphDefinition.
   */
  case class Result[TEMPLATE](executionGraphDefinition: ExecutionGraphDefinition, pipelineTemplates: Map[PipelineId, IndexedSeq[TEMPLATE]])

  /**
   * Builds an [[ExecutionGraphDefinition]], including [[PipelineDefinition]]s, for a given physical plan.
   */
  def defineFrom[TEMPLATE](breakingPolicy: PipelineBreakingPolicy,
                 operatorFuserFactory: OperatorFuserFactory[TEMPLATE],
                 physicalPlan: PhysicalPlan,
                 expressionConverters: ExpressionConverters,
                 leveragedOrders: LeveragedOrders): Result[TEMPLATE] = {

    val executionStateDefiner = new ExecutionStateDefiner(physicalPlan)
    val pipelineTreeBuilder = new PipelineTreeBuilder(breakingPolicy,
                                                      operatorFuserFactory,
                                                      executionStateDefiner,
                                                      physicalPlan.slotConfigurations,
                                                      physicalPlan.argumentSizes,
                                                      physicalPlan.applyPlans,
                                                      expressionConverters,
                                                      leveragedOrders)
    pipelineTreeBuilder.build(physicalPlan.logicalPlan)
    val egd = ExecutionGraphDefinition(physicalPlan,
      new ReadOnlyArray(executionStateDefiner.buffers.map(_.result).toArray),
      new ReadOnlyArray(executionStateDefiner.argumentStateMaps.toArray),
      pipelineTreeBuilder.pipelines.map(_.result),
      pipelineTreeBuilder.applyRhsPlans.toMap)
    val pipelineTemplates = pipelineTreeBuilder.pipelines.map(p => p.id -> p.pipelineTemplates).toMap

    Result(egd, pipelineTemplates)
  }
}
