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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherMath
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class Multiply(a: Expression, b: Expression) extends Expression  {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = (a(ctx, state), b(ctx, state)) match {
    case (x, y) if (x eq Values.NO_VALUE) || (y eq Values.NO_VALUE) => Values.NO_VALUE
    case (x,y) => CypherMath.multiply(x, y)
  }

  override def rewrite(f: (Expression) => Expression): Expression = f(Multiply(a.rewrite(f), b.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(a, b)

  override def symbolTableDependencies: Set[String] = a.symbolTableDependencies ++ b.symbolTableDependencies
}
