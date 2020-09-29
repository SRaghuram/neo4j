/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites CartesianProduct with leveraged order into a combination of Apply and PreserveOrder.
 */
case class cartesianProductLeveragedOrderToApplyPreserveOrder(cardinalities: Cardinalities,
                                                              providedOrders: ProvidedOrders,
                                                              leveragedOrders: LeveragedOrders,
                                                              parallelExecution: Boolean,
                                                              idGen: IdGen,
                                                              stopper: AnyRef => Boolean) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ CartesianProduct(lhs, rhs) if leveragedOrders.get(o.id) =>
      Apply(lhs, preserveOrder(rhs))(SameId(o.id))
  }, stopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private def preserveOrder(rhs: LogicalPlan): LogicalPlan = {
    if (parallelExecution) {
      val preserveOrder = PreserveOrder(rhs)(idGen)
      cardinalities.copy(rhs.id, preserveOrder.id)
      providedOrders.copy(rhs.id, preserveOrder.id)
      preserveOrder
    } else {
      // In single-threaded execution order is inherently preserved by Apply
      rhs
    }
  }
}
