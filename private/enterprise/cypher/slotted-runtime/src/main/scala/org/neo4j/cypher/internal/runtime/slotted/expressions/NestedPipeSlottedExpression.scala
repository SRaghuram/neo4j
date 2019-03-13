/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import java.util

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ExpressionVariable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
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

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val initialContext: SlottedExecutionContext = createInitialContext(ctx, state)
    val innerState = state.withInitialContext(initialContext).withDecorator(state.decorator.innerDecorator(owningPipe))

    val results = pipe.createResults(innerState)
    val all = new util.ArrayList[AnyValue]()
    while (results.hasNext) {
      all.add(inner(results.next(), state))
    }
    VirtualValues.fromList(all)
  }

  private def createInitialContext(ctx: ExecutionContext, state: QueryState): SlottedExecutionContext = {
    val initialContext = new SlottedExecutionContext(slots)
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

  override def symbolTableDependencies: Set[String] = Set()

  override def toString: String = s"NestedExpression()"
}
