/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ApplyPlans, ArgumentSizes, SlotConfigurations}
import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.expressionVariables
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan

object PhysicalPlanner {

  def plan(tokenContext: TokenContext,
           beforeRewrite: LogicalPlan,
           semanticTable: SemanticTable,
           breakingPolicy: PipelineBreakingPolicy): PhysicalPlan = {
    val (logicalPlan, nExpressionSlots) = expressionVariables.replace(beforeRewrite)
    val slotMetaData = SlotAllocation.allocateSlots(logicalPlan, semanticTable, breakingPolicy)
    val slottedRewriter = new SlottedRewriter(tokenContext)
    val finalLogicalPlan = slottedRewriter(logicalPlan, slotMetaData.slotConfigurations)
    PhysicalPlan(finalLogicalPlan,
                 nExpressionSlots,
                 slotMetaData.slotConfigurations,
                 slotMetaData.argumentSizes,
                 slotMetaData.applyPlans)
  }
}

case class PhysicalPlan(logicalPlan: LogicalPlan,
                        nExpressionSlots: Int,
                        slotConfigurations: SlotConfigurations,
                        argumentSizes: ArgumentSizes,
                        applyPlans: ApplyPlans)
