/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.planLimitOnTopOf
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites SemiApply into a combination of Limit and Apply, which are supported by the runtime.
 */
case class semiApplyToLimitApply(effectiveCardinalities: EffectiveCardinalities,
                                 providedOrders: ProvidedOrders,
                                 idGen: IdGen,
                                 stopper: AnyRef => Boolean) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ SemiApply(lhs: LogicalPlan, rhs: LogicalPlan) =>
      Apply(lhs, newRhs(lhs, rhs))(SameId(o.id))
    case o @ SelectOrSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, _) =>
      o.copy(right = newRhs(lhs, rhs))(SameId(o.id))
  }, stopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private def newRhs(lhs: LogicalPlan, rhs: LogicalPlan) = {
    val limit = planLimitOnTopOf(rhs, SignedDecimalIntegerLiteral("1")(InputPosition.NONE))(idGen)
    effectiveCardinalities.set(limit.id, effectiveCardinalities.get(lhs.id))
    providedOrders.copy(rhs.id, limit.id)
    limit
  }
}

