/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ErasedTwoChildrenPlan
import org.neo4j.cypher.internal.logical.plans.IndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.RewrittenPlans
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.runtime.pipelined.rewriters.combineCartesianProductOfMultipleIndexSeeks.CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUpWithRecorder

object combineCartesianProductOfMultipleIndexSeeks {
  // We only rewrite cartesian products if the left-hand-side cardinality is below this threshold
  // Since the MultiNodeIndexSeek operator currently does not cache the right-hand-side rows, the normal CartesianProduct operator that buffers morsels
  // is faster above a fairly low threshold (based on micro benchmarks)
  // However, we currently always do the rewrite if there is a provided order since the normal cartesian product operator cannot handle that and
  // would cause fallback to slotted runtime, and the MultiNodeIndexSeek operator is always slightly faster than using normal cartesian product in slotted.
  val CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD: Cardinality = Cardinality(16)
}

/**
  * Rewrite cartesian products of index seeks into a specialized multiple index seek operator
  */
case class combineCartesianProductOfMultipleIndexSeeks(cardinalities: Cardinalities,
                                                       providedOrders: ProvidedOrders,
                                                       rewrittenPlans: RewrittenPlans,
                                                       threshold: Cardinality = CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD) extends Rewriter {
  private val instance: Rewriter = bottomUpWithRecorder(Rewriter.lift {
    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: IndexSeekLeafPlan) if providedOrders.hasProvidedOrder(lhs.id) || cardinalities.get(lhs.id) < threshold =>
      MultiNodeIndexSeek(Array(lhs, rhs))(SameId(o.id))

    case o @ CartesianProduct(lhs: MultiNodeIndexSeek, rhs: IndexSeekLeafPlan) if providedOrders.hasProvidedOrder(lhs.id) || cardinalities.get(lhs.id) < threshold =>
      MultiNodeIndexSeek(lhs.nodeIndexSeeks :+ rhs)(SameId(o.id))

    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: MultiNodeIndexSeek) if providedOrders.hasProvidedOrder(lhs.id) || cardinalities.get(lhs.id) < threshold =>
      MultiNodeIndexSeek(lhs +: rhs.nodeIndexSeeks)(SameId(o.id))

  }, recorder = {
    // Record rewritten plans
    case (plan: LogicalPlan, rewrittenPlan: LogicalPlan) =>
      rewrittenPlans.set(plan.id, rewrittenPlan)
      // This will erase any previous child rewrites from the plan description, so that eventually only the top-most will be visible
      def eraser(p: Option[LogicalPlan]): Unit = p match {
        case Some(ep: MultiNodeIndexSeek) =>
          rewrittenPlans.set(ep.id, ErasedTwoChildrenPlan()(SameId(ep.id)))
        case _ => // Do nothing
      }
      eraser(plan.lhs)
      eraser(plan.rhs)
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
