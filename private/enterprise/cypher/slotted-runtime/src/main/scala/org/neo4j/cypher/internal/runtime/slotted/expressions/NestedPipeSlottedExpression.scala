/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import java.util

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

/**
 * Slotted variant of [[org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NestedPipeExpression]]
 */
case class NestedPipeSlottedExpression(pipe: Pipe,
                                       inner: Expression,
                                       slots: SlotConfiguration,
                                       availableExpressionVariables: Array[ExpressionVariable]) extends Expression {

  private val expVarSlotsInNestedPlan = availableExpressionVariables.map(ev => slots.getReferenceOffsetFor(ev.name))

  override def apply(ctx: ReadableRow, state: QueryState): AnyValue = {
    val initialContext: SlottedRow = createInitialContext(ctx, state)
    val innerState = state.withInitialContext(initialContext).withDecorator(state.decorator.innerDecorator(owningPipe))

    val results = pipe.createResults(innerState)
    val all = new util.ArrayList[AnyValue]()
    while (results.hasNext) {
      all.add(inner(results.next(), state))
    }
    VirtualValues.fromList(all)
  }

  private def createInitialContext(ctx: ReadableRow, state: QueryState): SlottedRow = {
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

  override def rewrite(f: Expression => Expression): Expression =
    f(NestedPipeSlottedExpression(pipe, inner.rewrite(f), slots, availableExpressionVariables))

  override def arguments = List(inner)

  override def toString: String = s"NestedExpression()"

  override def children: Seq[AstNode[_]] = Seq(inner) ++ availableExpressionVariables
}
