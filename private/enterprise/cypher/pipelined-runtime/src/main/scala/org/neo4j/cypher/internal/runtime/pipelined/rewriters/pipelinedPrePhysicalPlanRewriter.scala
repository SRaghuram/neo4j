/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.inSequence

/*
 * Rewriters that live here are used to either improve performance, or rewrite specialized
 * operators into a composition of simpler ones, to provide coverage in pipelined.
 */
case object pipelinedPrePhysicalPlanRewriter {

  def description: String = "optimize logical plans for pipelined execution using heuristic rewriting"

  def rewrite(effectiveCardinalities: EffectiveCardinalities,
              providedOrders: ProvidedOrders,
              leveragedOrders: LeveragedOrders,
              parallelExecution: Boolean,
              idGen: IdGen): AnyRef => AnyRef = {
    inSequence(
      fixedPoint(
        combineCartesianProductOfMultipleIndexSeeks(effectiveCardinalities, leveragedOrders, stopper = stopper)
      ),
      fixedPoint(
        assertSameNodeNodeRewriter(stopper = stopper)
      ),
      cartesianProductLeveragedOrderToApplyPreserveOrder(effectiveCardinalities, providedOrders, leveragedOrders, parallelExecution, idGen, stopper),

      // Temporarily disabled to win some time to handle a regression https://trello.com/c/R6Yc26LB/2349-regression-in-ldbcldbcsnbinteractive-write-summary-for-version-430-drop020
      //foreachApplyRewriter(effectiveCardinalities, providedOrders, idGen, stopper),//foreach rewrites to selectOrSemiApply so must come before semiApplyToLimitApply

      semiApplyToLimitApply(effectiveCardinalities, providedOrders, idGen, stopper),
      antiSemiApplyToAntiLimitApply(effectiveCardinalities, providedOrders, idGen, stopper),
      rollupApplyToAggregationApply(effectiveCardinalities, providedOrders, idGen, stopper),
      letAntiSemiApplyVariantsToAggregationLimitApply(effectiveCardinalities, providedOrders, idGen, stopper),
      letSemiApplyVariantsToAggregationLimitApply(effectiveCardinalities, providedOrders, idGen, stopper),
      triadicSelectionToBuildApplyFilter(effectiveCardinalities, providedOrders, idGen, stopper),
    )
  }

  private def stopper(a: AnyRef) = a match {
    case _: NestedPlanExpression => true
    case _ => false
  }

  def apply(query: LogicalQuery, parallelExecution: Boolean): LogicalPlan = {
    val inputPlan = query.logicalPlan
    val rewrittenPlan = inputPlan.endoRewrite(rewrite(query.effectiveCardinalities, query.providedOrders, query.leveragedOrders, parallelExecution, query.idGen))
    rewrittenPlan
  }
}
