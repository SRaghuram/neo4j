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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlanSelector
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds

import scala.annotation.tailrec

case class Selector(pickBestFactory: CandidateSelectorFactory,
                    candidateGenerators: SelectionCandidateGenerator*) extends PlanSelector {

  /**
   * Given a plan, using SelectionCandidateGenerators, plan all selections currently possible in the order cheapest first and return the resulting plan.
   */
  def apply(input: LogicalPlan, queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {
    val pickBest = pickBestFactory(context)

    val unsolvedPredicates = unsolvedPreds(context.planningAttributes.solveds, queryGraph.selections, input)

    @tailrec
    def selectIt(plan: LogicalPlan, stillUnsolvedPredicates: Set[Expression]): LogicalPlan = {
      val candidates = candidateGenerators.flatMap(generator => generator(plan, stillUnsolvedPredicates, queryGraph, interestingOrder, context))

      pickBest[SelectionCandidate](_.plan, candidates) match {
        case Some(SelectionCandidate(plan, solvedPredicates)) => selectIt(plan, stillUnsolvedPredicates -- solvedPredicates)
        case None => plan
      }
    }

    selectIt(input, unsolvedPredicates)
  }

  /**
   * All unsolved predicates. Includes scalar and pattern predicates.
   */
  private def unsolvedPreds(solveds: Solveds, s: Selections, l: LogicalPlan): Set[Expression] =
      s.predicatesGiven(l.availableSymbols)
        .filterNot(predicate => solveds.get(l.id).asSinglePlannerQuery.exists(_.queryGraph.selections.contains(predicate)))
        .toSet

}

// TODO instead of calling select for each candidate, lets only call it for the winner.
//  --> This is difficult in combination with the sorting generators since we want to plan Sort after Filter.
trait SelectionCandidateGenerator extends {
  /**
   * Generate candidates which solve a predicate.
   * @param input the current plan
   * @param unsolvedPredicates all predicates which are left to be solved
   * @param queryGraph the query graph to solve
   * @return candidates, where each candidate is a plan building on top of input that solves some predicates.
   */
  def apply(input: LogicalPlan,
            unsolvedPredicates: Set[Expression],
            queryGraph: QueryGraph,
            interestingOrder: InterestingOrder,
            context: LogicalPlanningContext): Iterator[SelectionCandidate]
}

case class SelectionCandidate(plan: LogicalPlan, solvedPredicates: Set[Expression])