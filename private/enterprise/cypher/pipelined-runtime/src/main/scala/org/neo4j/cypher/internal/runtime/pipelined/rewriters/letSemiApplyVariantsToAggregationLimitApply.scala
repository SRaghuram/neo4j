/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.physicalplanning.ast.NonEmpty
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites LetSemiApply variants into a combination of operators which are supported by pipelined.
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
case class letSemiApplyVariantsToAggregationLimitApply(cardinalities: Cardinalities,
                                                       providedOrders: ProvidedOrders,
                                                       idGen: IdGen) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o@LetSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, idName: String) =>
      val aggregation = rhsToAggregationLimit(lhs, rhs, idName)
      Apply(lhs, aggregation)(SameId(o.id))

    case o@LetSelectOrSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, idName: String, expr: Expression) =>
      val aggregation = rhsToAggregationLimit(lhs, rhs, idName)

      val proj = Projection(lhs, Map(idName -> True()(InputPosition.NONE)))(idGen)
      cardinalities.copy(lhs.id, proj.id)
      providedOrders.copy(lhs.id, proj.id)

      SelectOrSemiApply(proj, aggregation, expr)(SameId(o.id))
  })

  private def rhsToAggregationLimit(lhs: LogicalPlan,
                                    rhs: LogicalPlan,
                                    idName: String): LogicalPlan = {
    val limit = Limit(rhs, SignedDecimalIntegerLiteral("1")(InputPosition.NONE), DoNotIncludeTies)(idGen)
    val aggregation = Aggregation(limit, Map.empty, Map(idName -> NonEmpty()(InputPosition.NONE)))(idGen)
    cardinalities.copy(lhs.id, limit.id)
    cardinalities.copy(lhs.id, aggregation.id)
    providedOrders.copy(rhs.id, limit.id)
    providedOrders.copy(rhs.id, aggregation.id)
    aggregation
  }

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
