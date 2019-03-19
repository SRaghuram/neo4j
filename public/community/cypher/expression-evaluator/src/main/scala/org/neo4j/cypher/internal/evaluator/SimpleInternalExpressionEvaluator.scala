/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.evaluator

import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.Result
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{ExecutionContext, expressionVariableAllocation}
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Variable}
import org.neo4j.cypher.internal.v4_0.parser.CypherParser
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue

class SimpleInternalExpressionEvaluator extends InternalExpressionEvaluator {

  import SimpleInternalExpressionEvaluator.COLUMN

  private val parser = new CypherParser()

  override def evaluate(expression: String): AnyValue = {
    val e = parseToExpression(expression)
    val Result(rewritten, nExpressionSlots, _) = expressionVariableAllocation.allocate(e)
    val converters = new ExpressionConverters(CommunityExpressionConverter(TokenContext.EMPTY))
    val commandExpr = converters.toCommandExpression(Id.INVALID_ID, rewritten)

    val emptyQueryState = new QueryState(null,
                                         null,
                                         Array.empty,
                                         null,
                                         Array.empty[IndexReadSession],
                                         new Array(nExpressionSlots))
    try {
      commandExpr(ExecutionContext.empty, emptyQueryState)
    }
    catch {
      case e: Exception => throw new EvaluationException(s"Failed to evaluate expression $expression", e)
    }
  }

  private def parseToExpression(expression: String): Expression = {
    val statement: Statement = parser.parse(s"RETURN $expression AS $COLUMN", None)
    statement match {
      case Query(_,
                 SingleQuery(
                 Seq(Return(_, ReturnItems(_, Seq(AliasedReturnItem(e, Variable(COLUMN)))), _, _, _, _)))) =>
        e
      case _ => throw new EvaluationException(s"Invalid statement $statement")
    }
  }
}

object SimpleInternalExpressionEvaluator {

  private val COLUMN = "RESULT"
}
