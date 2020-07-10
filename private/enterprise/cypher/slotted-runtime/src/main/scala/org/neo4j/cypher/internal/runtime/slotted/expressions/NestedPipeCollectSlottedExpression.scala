/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

/**
 * Slotted variant of [[org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NestedPipeCollectExpression]]
 */
case class NestedPipeCollectSlottedExpression(pipe: Pipe,
                                              projection: Expression,
                                              slots: SlotConfiguration,
                                              availableExpressionVariables: Array[ExpressionVariable],
                                              owningPlanId: Id)
  extends NestedPipeSlottedExpression(pipe, slots, availableExpressionVariables, owningPlanId) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val results = createNestedResults(row, state)
    collectResults(state, results, projection)
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(NestedPipeCollectSlottedExpression(pipe, projection.rewrite(f), slots, availableExpressionVariables, owningPlanId))

  override def arguments = List(projection)

  override def children: Seq[AstNode[_]] = Seq(projection) ++ availableExpressionVariables
}
