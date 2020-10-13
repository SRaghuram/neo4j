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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins.planNIJ
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins.predicatesDependendingOnBothSides
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case class NestedIndexJoinComponentConnector(singleComponentPlanner: SingleComponentPlannerTrait)
  extends ComponentConnector {

  def solverStep(queryGraph: QueryGraph, interestingOrder: InterestingOrder, kit: QueryPlannerKit): ComponentConnectorSolverStep = {
    val predicatesWithDependencies: Array[(Expression, Array[String])] =
      queryGraph.selections.flatPredicates
                .map(pred => (pred, pred.dependencies.map(_.name).toArray))
                // A predicate can only join two components if it has at least 2 dependencies.
                .filter { case (_, deps) => deps.length > 1 }
                .toArray

    if (predicatesWithDependencies.isEmpty) {
      IDPSolverStep.empty[QueryGraph, LogicalPlan, LogicalPlanningContext]
    } else {
      (registry: IdRegistry[QueryGraph], goal: Goal, table: IDPCache[LogicalPlan], context: LogicalPlanningContext) => {
        for {
          // We cannot plan NIJ if the RHS is more than one component or optional matches because that would require us to recurse into
          // JoinDisconnectedQueryGraphComponents instead of SingleComponentPlannerTrait.
          rightGoal <- goal.subGoals(1)
          // Getting subGoals if size 1 can still give us compacted goals, so we need to post-filter the actual size
          if registry.explodedSize(rightGoal.bitSet) == 1
          rightPlan <- table(rightGoal).iterator

          containsOptionals = context.planningAttributes.solveds.get(rightPlan.id).asSinglePlannerQuery.lastQueryGraph.optionalMatches.nonEmpty
          if !containsOptionals

          rightQg = registry.explode(rightGoal.bitSet).reduce(_ ++ _)
          rightCovered = rightQg.allCoveredIds

          leftGoal = goal.diff(rightGoal)
          leftPlan <- table(leftGoal).iterator

          leftQg = registry.explode(leftGoal.bitSet).reduce(_ ++ _)
          leftCovered = leftQg.allCoveredIds

          predicate <- predicatesDependendingOnBothSides(predicatesWithDependencies, leftCovered, rightCovered)
          plan <- planNIJ(leftPlan, rightPlan, leftQg, rightQg, interestingOrder, predicate, context, kit, singleComponentPlanner)
        } yield plan
      }
    }
  }
}
