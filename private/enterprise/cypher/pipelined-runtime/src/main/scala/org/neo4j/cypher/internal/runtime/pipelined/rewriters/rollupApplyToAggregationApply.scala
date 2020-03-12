/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.InputPosition.NONE
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
  * Rewrites RollupApply into a combination of operators which are supported by pipelined.
  *
  * Rewrite
  *
  *    RollupApply
  *    LHS     RHS
  *
  * to (if it has no nullable variables)
  *
  *    Apply
  *      Aggregation
  *      RHS
  *    LHS
  *
  * or (if it has nullable variables)
  *
  *    Apply
  *      Optional
  *      Apply
  *        Aggregation
  *        RHS
  *      Selection
  *      Argument
  *    LHS
 */
case class rollupApplyToAggregationApply(cardinalities: Cardinalities,
                                         providedOrders: ProvidedOrders,
                                         idGen: IdGen) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ RollUpApply(lhs: LogicalPlan, rhs: LogicalPlan, collectionName, variableToCollect, nullableVariables) =>
      val toCollect = Variable(variableToCollect)(NONE)
      val aggregation = Aggregation(rhs, Map.empty, Map(collectionName -> Collect.asInvocation(toCollect)(NONE)))(idGen)
      cardinalities.copy(lhs.id, aggregation.id)
      providedOrders.copy(lhs.id, aggregation.id)

      if (nullableVariables.isEmpty) {
        Apply(lhs, aggregation)(SameId(o.id))
      } else {
        val argument = Argument()(idGen)
        val noNullVariables =
          Ands(nullableVariables.map(v => Not(IsNull(Variable(v)(NONE))(NONE))(NONE)))(NONE)
        val selection = Selection(noNullVariables, argument)(idGen)
        val innerApply = Apply(selection, aggregation)(idGen)
        val optional = Optional(innerApply, lhs.availableSymbols)(idGen)
        val outerApply = Apply(lhs, optional)(SameId(o.id))

        cardinalities.copy(lhs.id, argument.id)
        cardinalities.copy(lhs.id, selection.id)
        cardinalities.copy(lhs.id, innerApply.id)
        cardinalities.copy(lhs.id, optional.id)

        providedOrders.copy(lhs.id, argument.id)
        providedOrders.copy(lhs.id, selection.id)
        providedOrders.copy(lhs.id, innerApply.id)
        providedOrders.copy(lhs.id, optional.id)
        outerApply
      }
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
