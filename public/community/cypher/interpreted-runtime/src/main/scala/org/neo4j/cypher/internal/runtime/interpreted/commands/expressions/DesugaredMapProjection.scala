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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.{GraphElementPropertyFunctions, IsMap, LazyMap}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{MapValueBuilder, VirtualValues}

import scala.collection.Map

case class DesugaredMapProjection(variable: VariableCommand, includeAllProps: Boolean, literalExpressions: Map[String, Expression])
  extends Expression with GraphElementPropertyFunctions {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val variableValue = variable(ctx, state)

    val mapOfProperties = variableValue match {
      case v if v eq Values.NO_VALUE => return Values.NO_VALUE
      case IsMap(m) => if (includeAllProps) m(state) else VirtualValues.EMPTY_MAP
    }
    val builder = new MapValueBuilder(literalExpressions.size)
    literalExpressions.foreach {
      case (k, e) => builder.add(k, e(ctx, state))
    }

    //in case we get a lazy map we need to make sure it has been loaded
    mapOfProperties match {
      case m :LazyMap[_,_] => m.load()
      case _ =>
    }

    mapOfProperties.updatedWith(builder.build())
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(DesugaredMapProjection(variable, includeAllProps, literalExpressions.rewrite(f)))

  override def arguments: Seq[Expression] = literalExpressions.values.toIndexedSeq

  override def children: Seq[AstNode[_]] = Seq(variable) ++ arguments

  override def toString: String = s"$variable{.*, " + literalExpressions.mkString + "}"
}
