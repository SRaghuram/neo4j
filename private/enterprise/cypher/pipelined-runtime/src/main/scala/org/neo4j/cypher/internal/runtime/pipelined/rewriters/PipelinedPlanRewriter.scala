/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.helpers.fixedPoint

/*
 * Rewriters that live here are required to adhere to the contract of
 * receiving a valid plan and producing a valid plan. It should be possible
 * to disable any and all of these rewriters, and still produce correct behavior.
 */
case class PipelinedPlanRewriter(rewriterSequencer: String => RewriterStepSequencer) {

  def description: String = "optimize logical plans for pipelined execution using heuristic rewriting"

  def rewrite(cardinalities: Cardinalities, providedOrders: ProvidedOrders, idGen: IdGen, batchSize: Int): AnyRef => AnyRef = {
    fixedPoint(rewriterSequencer("PipelinedPlanRewriter")(
      combineCartesianProductOfMultipleIndexSeeks(cardinalities, providedOrders),
      semiApplyToLimitApply(cardinalities, providedOrders, idGen),
      ).rewriter)
  }

  def apply(query: LogicalQuery, batchSize: Int): LogicalPlan = {
    val inputPlan = query.logicalPlan
    val rewrittenPlan = inputPlan.endoRewrite(rewrite(query.cardinalities, query.providedOrders, query.idGen, batchSize))
    rewrittenPlan
  }
}

case object noopRewriter extends Rewriter {
  override def apply(input: AnyRef): AnyRef = input
}
