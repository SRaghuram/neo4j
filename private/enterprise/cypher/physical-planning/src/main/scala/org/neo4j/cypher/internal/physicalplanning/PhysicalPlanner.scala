/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.NestedPlanArgumentConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.Result
import org.neo4j.cypher.internal.runtime.slottedParameters

object PhysicalPlanner {

  def plan(tokenContext: TokenContext,
           beforeRewrite: LogicalPlan,
           semanticTable: SemanticTable,
           breakingPolicy: PipelineBreakingPolicy,
           leveragedOrders: LeveragedOrders,
           allocateArgumentSlots: Boolean = false): PhysicalPlan = {
    DebugSupport.PHYSICAL_PLANNING.log("======== BEGIN Physical Planning with %-31s ===========================", breakingPolicy.getClass.getSimpleName)
    val Result(logicalPlan, nExpressionSlots, availableExpressionVars) = expressionVariableAllocation.allocate(beforeRewrite)
    val (withSlottedParameters, parameterMapping) = slottedParameters(logicalPlan)
    val slotMetaData = SlotAllocation.allocateSlots(withSlottedParameters,
      semanticTable,
      breakingPolicy,
      availableExpressionVars,
      leveragedOrders,
      allocateArgumentSlots)
    val slottedRewriter = new SlottedRewriter(tokenContext)
    val finalLogicalPlan = slottedRewriter(withSlottedParameters, slotMetaData.slotConfigurations)
    DebugSupport.PHYSICAL_PLANNING.log("======== END Physical Planning ==================================================================")
    PhysicalPlan(finalLogicalPlan,
      nExpressionSlots,
      slotMetaData.slotConfigurations,
      slotMetaData.argumentSizes,
      slotMetaData.applyPlans,
      slotMetaData.nestedPlanArgumentConfigurations,
      availableExpressionVars,
      parameterMapping)
  }
}

case class PhysicalPlan(logicalPlan: LogicalPlan,
                        nExpressionSlots: Int,
                        slotConfigurations: SlotConfigurations,
                        argumentSizes: ArgumentSizes,
                        applyPlans: ApplyPlans,
                        nestedPlanArgumentConfigurations: NestedPlanArgumentConfigurations,
                        availableExpressionVariables: AvailableExpressionVariables,
                        parameterMapping: ParameterMapping)
