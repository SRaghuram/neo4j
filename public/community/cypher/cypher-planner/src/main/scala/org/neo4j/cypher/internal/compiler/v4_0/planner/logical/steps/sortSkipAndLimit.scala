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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v4_0.planner.logical._
import org.neo4j.cypher.internal.ir.v4_0._
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.ast.{AscSortItem, DescSortItem, SortItem}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Variable}
import org.neo4j.cypher.internal.v4_0.util.{FreshIdNameGenerator, InternalException}

object sortSkipAndLimit extends PlanTransformerWithRequiredOrder {

  /**
    * @param query            query.interestingOrder will be the one marked as solved.
    * @param interestingOrder this order can potentially be different from query.interestingOrder. It can contain renames and
    *                         will be used to check if we need to sort.
    */
  def apply(in: LogicalPlan, query: PlannerQuery, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {
    val plan = PlannerHelper.sortedPlanWithSolved(in, interestingOrder, context)
    query.horizon match {
      case p: QueryProjection =>
        val queryPagination = p.queryPagination
        (queryPagination.skip, queryPagination.limit) match {
          case (skip, limit) =>
            addLimit(limit, addSkip(skip, plan, context), context)
        }

      case _ => plan
    }
  }

  private def addSkip(s: Option[Expression], plan: LogicalPlan, context: LogicalPlanningContext) =
    s.fold(plan)(x => context.logicalPlanProducer.planSkip(plan, x, context))

  private def addLimit(s: Option[Expression], plan: LogicalPlan, context: LogicalPlanningContext) =
    s.fold(plan)(x => context.logicalPlanProducer.planLimit(plan, x, context = context))

}
