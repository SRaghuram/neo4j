/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NestedPipeExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Slotted variant of [[org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NestedPipeCollectExpression]]
 */
abstract class NestedPipeSlottedExpression(pipe: Pipe,
                                           slots: SlotConfiguration,
                                           availableExpressionVariables: Array[ExpressionVariable],
                                           owningPlanId: Id)
  extends NestedPipeExpression(pipe, availableExpressionVariables, owningPlanId) {

  private val expVarSlotsInNestedPlan = availableExpressionVariables.map(ev => slots.getReferenceOffsetFor(ev.name))

  override protected def createInitialContext(ctx: ReadableRow, state: QueryState): SlottedRow = {
    val initialContext = new SlottedRow(slots)
    initialContext.copyFrom(ctx, slots.numberOfLongs, slots.numberOfReferences - expVarSlotsInNestedPlan.length)
    var i = 0
    while (i < expVarSlotsInNestedPlan.length) {
      val expVar = availableExpressionVariables(i)
      initialContext.setRefAt(expVarSlotsInNestedPlan(i), state.expressionVariables(expVar.offset))
      i += 1
    }
    initialContext
  }
}
