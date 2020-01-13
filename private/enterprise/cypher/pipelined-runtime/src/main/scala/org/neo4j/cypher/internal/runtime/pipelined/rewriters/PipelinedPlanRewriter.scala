/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.RewrittenPlans
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.util.{Cardinality, Rewriter}
import org.neo4j.cypher.internal.util.helpers.fixedPoint

/*
 * Rewriters that live here are required to adhere to the contract of
 * receiving a valid plan and producing a valid plan. It should be possible
 * to disable any and all of these rewriters, and still produce correct behavior.
 */
case class PipelinedPlanRewriter(rewriterSequencer: String => RewriterStepSequencer) {
  def description: String = "optimize logical plans for pipelined execution using heuristic rewriting"

  def rewrite(cardinalities: Cardinalities, batchSize: Int, rewrittenPlans: RewrittenPlans): AnyRef => AnyRef = {
    fixedPoint(rewriterSequencer("PipelinedPlanRewriter")(
      combineCartesianProductOfMultipleIndexSeeks(threshold = Cardinality(batchSize), cardinalities, rewrittenPlans),
      noopRewriter // This is only needed to make rewriterSequencer happy when we have only a single rewriter enabled
      ).rewriter)
  }

  def apply(inputPlan: LogicalPlan, cardinalities: Cardinalities, batchSize: Int, rewrittenPlans: RewrittenPlans): LogicalPlan = {
    val rewrittenPlan = inputPlan.endoRewrite(rewrite(cardinalities, batchSize, rewrittenPlans))
    rewrittenPlan
  }
}

case object noopRewriter extends Rewriter {
  override def apply(input: AnyRef): AnyRef = input
}
