/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.{CartesianProduct, ErasedTwoChildrenPlan, IndexSeekLeafPlan, LogicalPlan, MultiNodeIndexSeek}
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.RewrittenPlans
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.{Cardinality, Rewriter, bottomUpWithRecorder}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities

/**
  * Rewrite cartesian products of index seeks into a specialized multiple index seek operator
  */
case class combineCartesianProductOfMultipleIndexSeeks(threshold: Cardinality, cardinalities: Cardinalities, rewrittenPlans: RewrittenPlans) extends Rewriter {

  private val instance: Rewriter = bottomUpWithRecorder(Rewriter.lift {
    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: IndexSeekLeafPlan) if cardinalities.get(rhs.id) < threshold =>
      MultiNodeIndexSeek(Array(lhs, rhs))(SameId(o.id))

    case o @ CartesianProduct(lhs: MultiNodeIndexSeek, rhs: IndexSeekLeafPlan) if cardinalities.get(rhs.id) < threshold =>
      MultiNodeIndexSeek(lhs.nodeIndexSeeks :+ rhs)(SameId(o.id))

    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: MultiNodeIndexSeek) if cardinalities.get(rhs.id) < threshold =>
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
