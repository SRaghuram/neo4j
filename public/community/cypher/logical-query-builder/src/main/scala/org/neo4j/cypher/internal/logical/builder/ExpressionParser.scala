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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.parser.Expressions
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, Rewriter, topDown}
import org.parboiled.scala.{ReportingParseRunner, Rule1}

object ExpressionParser {
  val injectCachedNodeProperties: Rewriter = topDown(Rewriter.lift {
    case ContainerIndex(Variable("cache"), Property(v@Variable(node), pkn:PropertyKeyName)) =>
      CachedProperty(node, v, pkn, NODE_TYPE)(AbstractLogicalPlanBuilder.pos)
  })
  val invalidateInputPositions: Rewriter = topDown(Rewriter.lift {
    case a:ASTNode => a.dup(a.children.toSeq :+ AbstractLogicalPlanBuilder.pos)
  })

  private val regex = s"(.+) AS ([a-zA-Z0-9]+)".r
  private val expParser = new ExpressionParser

  def parseProjections(projections: String*): Map[String, Expression] = {
    projections.map {
      case regex(ExpressionParser(expression), alias) => (alias,
        injectCachedNodeProperties.andThen(invalidateInputPositions)(expression).asInstanceOf[Expression])
      case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as a projection")
    }.toMap
  }

  def parseExpression(text: String): Expression = expParser.parse(text)

  def unapply(arg: String): Option[Expression] = Some(expParser.parse(arg))

}

class ExpressionParser extends Expressions {
  private val parser: Rule1[Expression] = Expression

  def parse(text: String): Expression = {
    val res = ReportingParseRunner(parser).run(text)
    res.result match {
      case Some(e) => e
      case None => throw new IllegalArgumentException(s"Could not parse expression: ${res.parseErrors}")
    }
  }
}
