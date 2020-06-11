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
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.ast.NonEmpty
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites LetSemiApply into a combination of operators which are supported by pipelined.
 *
 * Rewrite
 *
 *    LetSemiApply
 *    LHS     RHS
 *
 * to
 *
 *    Apply
 *      Aggregation(nonEmpty)
 *      Limit(1)
 *      RHS
 *    LHS
 */
case class letSemiApplyToAggregationLimitApply(cardinalities: Cardinalities,
                                               providedOrders: ProvidedOrders,
                                               idGen: IdGen) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o@LetSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, idName: String) =>
      val limit = Limit(rhs, SignedDecimalIntegerLiteral("1")(InputPosition.NONE), DoNotIncludeTies)(idGen)
      val aggregation = Aggregation(limit, Map.empty, Map(idName -> NonEmpty()(InputPosition.NONE)))(idGen)
      cardinalities.copy(lhs.id, limit.id)
      cardinalities.copy(lhs.id, aggregation.id)
      providedOrders.copy(rhs.id, limit.id)
      providedOrders.copy(rhs.id, aggregation.id)

      Apply(lhs, aggregation)(SameId(o.id))
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
