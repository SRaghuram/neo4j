/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.runtime.pipelined.rewriters.combineCartesianProductOfMultipleIndexSeeks.CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

object combineCartesianProductOfMultipleIndexSeeks {
  // We only rewrite cartesian products if the left-hand-side cardinality is below this threshold
  // Since the MultiNodeIndexSeek operator currently does not cache the right-hand-side rows, the normal CartesianProduct operator that buffers morsels
  // is faster above a fairly low threshold (based on micro benchmarks)
  // However, we currently always do the rewrite if there is a leveraged order since the normal cartesian product operator cannot handle that and
  // would cause fallback to slotted runtime, and the MultiNodeIndexSeek operator is always slightly faster than using normal cartesian product in slotted.
  val CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD: EffectiveCardinality = EffectiveCardinality(16)
}

/**
  * Rewrite cartesian products of index seeks into a specialized multiple index seek operator
  */
case class combineCartesianProductOfMultipleIndexSeeks(effectiveCardinalities: EffectiveCardinalities,
                                                       leveragedOrders: LeveragedOrders,
                                                       threshold: EffectiveCardinality = CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD,
                                                       stopper: AnyRef => Boolean) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ CartesianProduct(lhs: NodeIndexSeekLeafPlan, rhs: NodeIndexSeekLeafPlan) if leveragedOrders.get(o.id) || effectiveCardinalities.get(lhs.id) < threshold =>
      MultiNodeIndexSeek(Array(lhs, rhs))(SameId(o.id))

    case o @ CartesianProduct(lhs: MultiNodeIndexSeek, rhs: NodeIndexSeekLeafPlan) if leveragedOrders.get(o.id) || effectiveCardinalities.get(lhs.id) < threshold =>
      MultiNodeIndexSeek(lhs.nodeIndexSeeks :+ rhs)(SameId(o.id))

    case o @ CartesianProduct(lhs: NodeIndexSeekLeafPlan, rhs: MultiNodeIndexSeek) if leveragedOrders.get(o.id) || effectiveCardinalities.get(lhs.id) < threshold =>
      MultiNodeIndexSeek(lhs +: rhs.nodeIndexSeeks)(SameId(o.id))

  }, stopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
