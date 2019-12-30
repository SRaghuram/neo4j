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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

/**
  * For every row in left, set that row as the argument, and apply to right. For each right row,
  * produce current left row merged with current right row.
  *
  * for ( leftRow <- left ) {
  *   right.setArgument( leftRow )
  *   for ( rightRow <- right ) {
  *     produce (leftRow merge rightRow)
  *   }
  * }
  */
case class CrossApply(left: LogicalPlan, right: LogicalPlan)(implicit idGen: IdGen) extends BasicApply(idGen) {

  val lhs: Option[LogicalPlan] = Some(left)
  val rhs: Option[LogicalPlan] = Some(right)

  override def createNew(left: LogicalPlan, right: LogicalPlan, idGen: IdGen): CrossApply =
    CrossApply(left, right)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}
