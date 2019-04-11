/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, PhysicalPlan, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{QueryIndexes, slotted}
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.{createProjectionsForResult, translateColumnOrder}
import org.neo4j.cypher.internal.runtime.zombie.operators._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
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
      case plans.Input(nodes, variables, _) =>
        new InputOperator(WorkIdentity.fromPlan(plan),
                          nodes.map(v => slots.getLongOffsetFor(v)),
                          variables.map(v => slots.getReferenceOffsetFor(v)))

      case plans.AllNodesScan(column, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new AllNodeScanOperator(WorkIdentity.fromPlan(plan),
                                slots.getLongOffsetFor(column),
                                argumentSize)

      case plans.NodeByLabelScan(column, label, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        queryIndexes.registerLabelScan()
        new LabelScanOperator(WorkIdentity.fromPlan(plan),
                              slots.getLongOffsetFor(column),
                              LazyLabel(label)(SemanticTable()),
                              argumentSize)

      case plans.NodeIndexSeek(column, label, properties, valueExpr, _,  indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(WorkIdentity.fromPlan(plan),
                                  slots.getLongOffsetFor(column),
                                  label,
                                  properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
                                  queryIndexes.registerQueryIndex(label, properties),
                                  indexOrder,
                                  argumentSize,
                                  valueExpr.map(converters.toCommandExpression(id, _)),
                                  indexSeekMode)

      case plans.NodeUniqueIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(WorkIdentity.fromPlan(plan),
                                  slots.getLongOffsetFor(column),
                                  label,
                                  properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
                                  queryIndexes.registerQueryIndex(label, properties),
                                  indexOrder,
                                  argumentSize,
                                  valueExpr.map(converters.toCommandExpression(id, _)),
                                  indexSeekMode)

      case plans.Expand(lhs, fromName, dir, types, to, relName, plans.ExpandAll) =>
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

  def createMiddle(plan: LogicalPlan): Option[MiddleOperator] = {
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
      case plans.Selection(predicate, _) =>
        Some(new FilterOperator(WorkIdentity.fromPlan(plan), converters.toCommandExpression(id, predicate)))

      case plans.Limit(_, count, ties) =>
        Some(new LimitOperator(plan.id, WorkIdentity.fromPlan(plan), converters.toCommandExpression(plan.id, count)))

      case plans.Projection(_, expressions) =>
        val projectionOps = expressions.map {
          case (key, e) => slots(key) -> converters.toCommandExpression(id, e)
        }
        Some(new ProjectOperator(WorkIdentity.fromPlan(plan), projectionOps))

      case _: plans.Argument => None
    }
  }

  def createProduceResults(plan: plans.ProduceResult): ProduceResultOperator = {
    val slots = physicalPlan.slotConfigurations(plan.id)
    val runtimeColumns = createProjectionsForResult(plan.columns, slots)
    new ProduceResultOperator(WorkIdentity.fromPlan(plan),
                              slots,
                              runtimeColumns)
  }
}
