/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.compiler.helpers.AggregationHelper
import org.neo4j.cypher.internal.compiler.planner.logical.PlanSingleQuery.addAggregatedPropertiesToContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.compiler.planner.logical.steps.countStorePlanner
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

import scala.annotation.tailrec

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(planPart: MatchPlanner = planMatch,
                           planEventHorizon: EventHorizonPlanner = PlanEventHorizon,
                           planWithTail: TailPlanner = PlanWithTail(),
                           planUpdates:UpdatesPlanner = PlanUpdates)
  extends SingleQueryPlanner {

  override def apply(in: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val updatedContext = addAggregatedPropertiesToContext(in, context)

    val plans = countStorePlanner(in, updatedContext) match {
      case Some(plan) =>
        Seq(plan)
      case None =>
        val matchPlans = planPart(in, updatedContext)
        // We take all plans solving the MATCH part. This could be two, if we have a required order.
        val plansWithHorizon = matchPlans.allResults.map(planUpdatesInputAndHorizon(_, in, updatedContext))
        // `distinct` to skip planning subsequent query parts twice if the best overall plan and the best sorted plan happen to be be the same.
        plansWithHorizon.toSeq.distinct
      }

    val contextForTail = updatedContext.withUpdatedLabelInfo(plans.head) // cardinality should be the same for all plans, let's use the first one
    val (plan, _) = planWithTail(plans, in, contextForTail)
    plan
  }

  /**
   * Plan updates, query input, and horizon for all of them.
   * Horizon planning will ensure that any ORDER BY clause is solved, so in the end we have up to two plans that are comparable.
   */
  private def planUpdatesInputAndHorizon(matchPlan: LogicalPlan, in: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val planWithUpdates = planUpdates(in, matchPlan, firstPlannerQuery = true, context)

    val planWithInput = in.queryInput match {
      case Some(variables) =>
        val inputPlan = context.logicalPlanProducer.planInput(variables, context)
        context.logicalPlanProducer.planInputApply(inputPlan, planWithUpdates, variables, context)
      case None => planWithUpdates
    }

    planEventHorizon(in, planWithInput, None, context)
  }
}

object PlanSingleQuery {
  /*
   * Extract all properties over which aggregation is performed, where we potentially could use a NodeIndexScan.
   */
  def addAggregatedPropertiesToContext(currentQuery: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlanningContext = {

    // The renamings map is used to keep track of any projections changing the name of the property,
    // as in MATCH (n:Label) WITH n.prop1 AS prop RETURN count(prop)
    @tailrec
    def rec(currentQuery: SinglePlannerQuery, context: LogicalPlanningContext, renamings: Map[String, Expression]): LogicalPlanningContext = {
      // If the graph is mutated between the MATCH and the aggregation, an index scan might lead to the wrong number of mutations
      if (currentQuery.queryGraph.mutatingPatterns.nonEmpty) return context

      currentQuery.horizon match {
        case aggr: AggregatingQueryProjection =>
          if (aggr.groupingExpressions.isEmpty) // needed here to not enter next case
            AggregationHelper.extractProperties(aggr.aggregationExpressions, renamings) match {
              case properties: Set[(String, String)] if properties.nonEmpty => context.withAggregationProperties(properties)
              case _ => context
            }
          else context
        case proj: QueryProjection =>
          currentQuery.tail match {
            case Some(tail) =>
              rec(tail, context, renamings ++ proj.projections)
            case _ => context
          }
        case _ =>
          currentQuery.tail match {
            case Some(tail) => rec(tail, context, renamings)
            case _ => context
          }
      }
    }

    rec(currentQuery, context, Map.empty)
  }
}

trait MatchPlanner {
  def apply(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean = false): BestPlans
}

trait EventHorizonPlanner {
  def apply(query: SinglePlannerQuery, plan: LogicalPlan, previousInterestingOrder: Option[InterestingOrder], context: LogicalPlanningContext): LogicalPlan
}

trait TailPlanner {
  def apply(lhsPlans: Seq[LogicalPlan], in: SinglePlannerQuery, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext)
}

trait UpdatesPlanner {
  def apply(query: SinglePlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext): LogicalPlan
}
