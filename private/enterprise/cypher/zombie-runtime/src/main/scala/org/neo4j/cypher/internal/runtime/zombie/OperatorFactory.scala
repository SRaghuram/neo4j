/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.zombie.operators.{AllNodeScanOperator, ExpandAllOperator, ProduceResultOperator, StreamingOperator}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.createProjectionsForResult
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans._

class OperatorFactory(physicalPlan: PhysicalPlan,
                      converters: ExpressionConverters,
                      readOnly: Boolean,
                      queryIndexes: QueryIndexes) {

  def create(plan: LogicalPlan): StreamingOperator = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.AllNodesScan(column, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new AllNodeScanOperator(
          WorkIdentity.fromPlan(plan),
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

      case plan: plans.ProduceResult => createProduceResults(plan)
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
