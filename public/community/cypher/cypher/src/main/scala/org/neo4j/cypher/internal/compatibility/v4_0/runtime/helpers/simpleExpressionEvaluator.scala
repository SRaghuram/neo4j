/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v4_0.runtime.helpers

import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.{CypherException => InternalCypherException}

case object simpleExpressionEvaluator extends ExpressionEvaluator {

  // Returns Some(value) if the expression can be independently evaluated in an empty context/query state, otherwise None
  def evaluateExpression(expr: Expression): Option[Any] = {
    val converters = new ExpressionConverters(CommunityExpressionConverter(TokenContext.EMPTY))
    val commandExpr = converters.toCommandExpression(Id.INVALID_ID, expr)

    val emptyQueryState = new QueryState(null, null, VirtualValues.EMPTY_MAP, null, Array.empty[IndexReadSession])

    try {
      Some(commandExpr(ExecutionContext.empty, emptyQueryState))
    }
    catch {
      case e: InternalCypherException => None // Silently disregard expressions that cannot be evaluated in an empty context
    }
  }
}
