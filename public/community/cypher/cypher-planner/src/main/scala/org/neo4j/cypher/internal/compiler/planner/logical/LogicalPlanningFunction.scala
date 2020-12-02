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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

trait PlanSelector {
  def apply(plan: LogicalPlan, queryGraph: QueryGraph, interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): LogicalPlan
}

trait PlanTransformer {
  def apply(plan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan
}

trait CandidateSelector extends ProjectingSelector[LogicalPlan]

trait LeafPlanner {
  def apply(queryGraph: QueryGraph, interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): Seq[LogicalPlan]
}

object LeafPlansForVariable {
  def maybeLeafPlans(id: String, plans: Set[LogicalPlan]): Option[LeafPlansForVariable] =
    if (plans.isEmpty) None else Some(LeafPlansForVariable(id, plans))
}

case class LeafPlansForVariable(id: String, plans: Set[LogicalPlan]) {
  require(plans.nonEmpty)
}

trait LeafPlanFromExpressions {
  def producePlanFor(predicates: Set[Expression],
                     qg: QueryGraph,
                     interestingOrderConfig: InterestingOrderConfig,
                     context: LogicalPlanningContext): Set[LeafPlansForVariable]
}

trait LeafPlanFromExpression extends LeafPlanFromExpressions {

  def producePlanFor(e: Expression,
                     qg: QueryGraph,
                     interestingOrderConfig: InterestingOrderConfig,
                     context: LogicalPlanningContext): Option[LeafPlansForVariable]

  override def producePlanFor(predicates: Set[Expression],
                              qg: QueryGraph,
                              interestingOrderConfig: InterestingOrderConfig,
                              context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    predicates.flatMap(p => producePlanFor(p, qg, interestingOrderConfig, context))
  }
}

/**
 * Finds the best sorted and unsorted plan for every unique set of available symbols.
 */
trait LeafPlanFinder {
  def apply(config: QueryPlannerConfiguration,
            queryGraph: QueryGraph,
            interestingOrderConfig: InterestingOrderConfig,
            context: LogicalPlanningContext): Iterable[BestPlans]
}

sealed trait LeafPlanRestrictions {
  def symbolsThatShouldOnlyUseIndexLeafPlanners: Set[String]
}

object LeafPlanRestrictions {
  case object NoRestrictions extends LeafPlanRestrictions {
    override def symbolsThatShouldOnlyUseIndexLeafPlanners: Set[String] = Set.empty
  }

  case class OnlyIndexPlansFor(variable: String, dependencies: Set[String]) extends LeafPlanRestrictions {
    override def symbolsThatShouldOnlyUseIndexLeafPlanners: Set[String] = Set(variable)
  }

}
