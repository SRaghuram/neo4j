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
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, Rewriter}
import org.neo4j.cypher.internal.v4_0.util.helpers.fixedPoint
/*
 * Rewriters that live here are required to adhere to the contract of
 * receiving a valid plan and producing a valid plan. It should be possible
 * to disable any and all of these rewriters, and still produce correct behavior.
 */
case class PipelinedPlanRewriter(rewriterSequencer: String => RewriterStepSequencer) {
  def description: String = "optimize logical plans for pipelined execution using heuristic rewriting"

  def rewrite(cardinalities: Cardinalities, batchSize: Int): AnyRef => AnyRef = {
    fixedPoint(rewriterSequencer("PipelinedPlanRewriter")(
      combineCartesianProductOfMultipleIndexSeeks(cardinalities, threshold = Cardinality(batchSize)),
      noopRewriter // This is only needed to make rewriterSequencer happy when we have only one rewriter enabled
      ).rewriter)
  }

  def apply(inputPlan: LogicalPlan, cardinalities: Cardinalities, batchSize: Int): LogicalPlan = {
    val rewrittenPlan = inputPlan.endoRewrite(rewrite(cardinalities, batchSize))
    rewrittenPlan
  }
}


case object noopRewriter extends Rewriter {
  override def apply(input: AnyRef): AnyRef = input
}
