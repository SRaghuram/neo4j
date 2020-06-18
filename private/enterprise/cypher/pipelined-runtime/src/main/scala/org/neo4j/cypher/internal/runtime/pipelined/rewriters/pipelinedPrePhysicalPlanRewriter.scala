/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
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

  def rewrite(cardinalities: Cardinalities,
              providedOrders: ProvidedOrders,
              leveragedOrders: LeveragedOrders,
              idGen: IdGen): AnyRef => AnyRef = {
    inSequence(
      fixedPoint(
        combineCartesianProductOfMultipleIndexSeeks(cardinalities, leveragedOrders)
      ),
      semiApplyToLimitApply(cardinalities, providedOrders, idGen),
      antiSemiApplyToAntiLimitApply(cardinalities, providedOrders, idGen),
      rollupApplyToAggregationApply(cardinalities, providedOrders, idGen),
      letSemiApplyVariantsToAggregationLimitApply(cardinalities, providedOrders, idGen),
      letAntiSemiApplyToAggregationLimitApply(cardinalities, providedOrders, idGen)
    )
  }

  def apply(query: LogicalQuery): LogicalPlan = {
    val inputPlan = query.logicalPlan
    val rewrittenPlan = inputPlan.endoRewrite(rewrite(query.cardinalities, query.providedOrders, query.leveragedOrders, query.idGen))
    rewrittenPlan
  }
}
