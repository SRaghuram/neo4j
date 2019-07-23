/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ApplyPlans, ArgumentSizes, NestedPlanArgumentConfigurations, SlotConfigurations}
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.{AvailableExpressionVariables, Result}
import org.neo4j.cypher.internal.runtime.{ParameterMapping, expressionVariableAllocation, slottedParameters}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable

object PhysicalPlanner {

  def plan(tokenContext: TokenContext,
           beforeRewrite: LogicalPlan,
           semanticTable: SemanticTable,
           breakingPolicy: PipelineBreakingPolicy,
           transactionMaxMemory: Long,
           allocateArgumentSlots: Boolean = false): PhysicalPlan = {
    val Result(logicalPlan, nExpressionSlots, availableExpressionVars) = expressionVariableAllocation.allocate(beforeRewrite)
    val (withSlottedParameters, parameterMapping) = slottedParameters(logicalPlan)
    val slotMetaData = SlotAllocation.allocateSlots(withSlottedParameters, semanticTable, breakingPolicy, availableExpressionVars, allocateArgumentSlots)
    val slottedRewriter = new SlottedRewriter(tokenContext)
    val finalLogicalPlan = slottedRewriter(withSlottedParameters, slotMetaData.slotConfigurations)
    PhysicalPlan(finalLogicalPlan,
                 nExpressionSlots,
                 slotMetaData.slotConfigurations,
                 slotMetaData.argumentSizes,
                 slotMetaData.applyPlans,
                 slotMetaData.nestedPlanArgumentConfigurations,
                 availableExpressionVars,
                 parameterMapping,
                 transactionMaxMemory)
  }
}

case class PhysicalPlan(logicalPlan: LogicalPlan,
                        nExpressionSlots: Int,
                        slotConfigurations: SlotConfigurations,
                        argumentSizes: ArgumentSizes,
                        applyPlans: ApplyPlans,
                        nestedPlanArgumentConfigurations: NestedPlanArgumentConfigurations,
                        availableExpressionVariables: AvailableExpressionVariables,
                        parameterMapping: ParameterMapping,
                        transactionMaxMemory: Long)
