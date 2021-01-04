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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.ir.ordering.InterestingOrder

/**
 * When there is an ORDER BY later in the query, it can make sense to solve it earlier.
 * In that case, we are not required to solve it and cannot mark it as solved.
 * To account for this, we need to track both the order to report (which is the one required to solve right now)
 * and the one to solve, which can come from a later point of the query.
 */
final case class InterestingOrderConfig(orderToReport: InterestingOrder, orderToSolve: InterestingOrder)

object InterestingOrderConfig {
  /**
   * An InterestingOrderConfig with the same order to report and order to solve.
   */
  def apply(orderToReportAndSolve: InterestingOrder): InterestingOrderConfig =
    InterestingOrderConfig(orderToReportAndSolve, orderToReportAndSolve)

  val empty: InterestingOrderConfig = InterestingOrderConfig(InterestingOrder.empty)
}