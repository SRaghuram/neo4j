/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

/**
 * Expression that executes a pipe on evaluation. Returns true if the pipe produces rows, and false if no rows are produced.
 */
case class NestedPipeExistsExpression(pipe: Pipe,
                                      availableExpressionVariables: Seq[ExpressionVariable],
                                      owningPlanId: Id) extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {

    val initialContext = pipe.executionContextFactory.copyWith(row)
    availableExpressionVariables.foreach { expVar =>
      initialContext.set(expVar.name, state.expressionVariables(expVar.offset))
    }
    val innerState = state.withInitialContext(initialContext)
                          .withDecorator(state.decorator.innerDecorator(owningPlanId))

    val results = pipe.createResults(innerState)
    Values.booleanValue(results.hasNext)
  }

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = availableExpressionVariables

  override def toString: String = s"NestedExpression()"
}
