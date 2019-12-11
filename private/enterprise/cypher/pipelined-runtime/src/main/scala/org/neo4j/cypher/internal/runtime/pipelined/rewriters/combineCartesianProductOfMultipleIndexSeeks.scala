package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.{CartesianProduct, IndexSeekLeafPlan, MultiNodeIndexSeek}
import org.neo4j.cypher.internal.v4_0.util.attribution.SameId
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, Rewriter, bottomUp}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities

/**
  * Rewrite cartesian products of index seeks into a specialized multiple index seek operator
  */
case class combineCartesianProductOfMultipleIndexSeeks(cardinalities: Cardinalities, threshold: Cardinality) extends Rewriter {

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: IndexSeekLeafPlan) if cardinalities.get(rhs.id) < threshold =>
      MultiNodeIndexSeek(Array(lhs, rhs))(SameId(o.id))

    case o @ CartesianProduct(lhs: MultiNodeIndexSeek, rhs: IndexSeekLeafPlan) if cardinalities.get(rhs.id) < threshold =>
      MultiNodeIndexSeek(lhs.nodeIndexSeeks :+ rhs)(SameId(o.id))

    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: MultiNodeIndexSeek) if cardinalities.get(rhs.id) < threshold =>
      MultiNodeIndexSeek(lhs +: rhs.nodeIndexSeeks)(SameId(o.id))
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
