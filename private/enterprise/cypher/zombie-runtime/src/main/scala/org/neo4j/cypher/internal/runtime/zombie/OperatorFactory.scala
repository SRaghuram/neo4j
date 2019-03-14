/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.{LongSlot, PhysicalPlan}
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.zombie.operators._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.{createProjectionsForResult, translateColumnOrder}
import org.neo4j.cypher.internal.runtime.slotted
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.symbols.CTInteger

class OperatorFactory(physicalPlan: PhysicalPlan,
                      converters: ExpressionConverters,
                      readOnly: Boolean,
                      queryIndexes: QueryIndexes) {

  def create(plan: LogicalPlan): Operator = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.AllNodesScan(column, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new AllNodeScanOperator(WorkIdentity.fromPlan(plan),
                                slots.getLongOffsetFor(column),
                                argumentSize)

      case plans.Expand(lhs, fromName, dir, types, to, relName, ExpandAll) =>
        val fromOffset = slots.getLongOffsetFor(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(to)
        val lazyTypes = LazyTypes(types.toArray)(SemanticTable())
        new ExpandAllOperator(WorkIdentity.fromPlan(plan),
                              fromOffset,
                              relOffset,
                              toOffset,
                              dir,
                              lazyTypes)

      case plans.Sort(_, sortItems) =>
        val ordering = sortItems.map(translateColumnOrder(slots, _))
        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)
        new SortMergeOperator(id,
                              WorkIdentity.fromPlan(plan),
                              ordering,
                              argumentSlot)

      case plan: plans.ProduceResult => createProduceResults(plan)

      case _: plans.Argument =>
        new ArgumentOperator(WorkIdentity.fromPlan(plan),
                             physicalPlan.argumentSizes(id))
    }
  }

  def createMiddle(plan: LogicalPlan): Option[StatelessOperator] = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.Sort(_, sortItems) =>
        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)
        val argumentOrdering = slotted.Ascending(LongSlot(argumentSlot, nullable = false, CTInteger))
        val ordering = argumentOrdering +: sortItems.map(translateColumnOrder(slots, _))
        Some(new SortPreOperator(WorkIdentity.fromPlan(plan),
                                 ordering))

      case _: plans.Argument => None
    }
  }

  def createProduceResults(plan: ProduceResult): ProduceResultOperator = {
    val slots = physicalPlan.slotConfigurations(plan.id)
    val runtimeColumns = createProjectionsForResult(plan.columns, slots)
    new ProduceResultOperator(WorkIdentity.fromPlan(plan),
                              slots,
                              runtimeColumns)
  }
}
