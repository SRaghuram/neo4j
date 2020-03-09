/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites SemiApply into a combination of Limit and Apply, which are supported by the runtime.
 */
case class semiApplyToLimitApply(cardinalities: Cardinalities,
                                 providedOrders: ProvidedOrders,
                                 idGen: IdGen) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ SemiApply(lhs: LogicalPlan, rhs: LogicalPlan) =>
      val limit = Limit(rhs, SignedDecimalIntegerLiteral("1")(InputPosition.NONE), DoNotIncludeTies)(idGen)
      cardinalities.set(limit.id, Cardinality.min(cardinalities.get(rhs.id), Cardinality.SINGLE))
      providedOrders.copy(rhs.id, limit.id)
      Apply(lhs, limit)(SameId(o.id))
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
