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
package org.neo4j.cypher.internal.v4_0.logical.plans

import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.ir.v4_0.StrictnessMode
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

/**
  * for ( row <- source )
  *   node = row.get(idName)
  *   for ( (key,value) <- row.evaluate( expression ) )
  *     node.setProperty( key, value )
  *
  *   produce row
  */
case class SetNodePropertiesFromMap(
                                     source: LogicalPlan,
                                     idName: String,
                                     expression: Expression,
                                     removeOtherProps: Boolean
                                   )(implicit idGen: IdGen)
  extends LogicalPlan(idGen) {

  override def lhs: Option[LogicalPlan] = Some(source)

  override val availableSymbols: Set[String] = source.availableSymbols + idName

  override def rhs: Option[LogicalPlan] = None

  override def strictness: StrictnessMode = source.strictness
}
