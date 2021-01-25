/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites
 *
 *   TriadicSelection
 *     RHS
 *   LHS
 *
 * into
 *
 *   TriadicFilter
 *   Apply
 *     RHS
 *   TriadicBuild
 *   LHS
 */
case class triadicSelectionToBuildApplyFilter(cardinalities: Cardinalities,
                                              providedOrders: ProvidedOrders,
                                              idGen: IdGen,
                                              stopper: AnyRef => Boolean) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case triSelection @ TriadicSelection(lhs, rhs, positivePredicate, sourceId, seenId, targetId) =>
      val triBuild = TriadicBuild(lhs, sourceId, seenId, Some(triSelection.id))(idGen)
      cardinalities.copy(from = lhs.id, to = triBuild.id)
      providedOrders.copy(from = lhs.id, to = triBuild.id)

      val apply = Apply(triBuild, rhs)(idGen)
      cardinalities.copy(from = rhs.id, to = apply.id)
      providedOrders.copy(from = rhs.id, to = apply.id)

      val triFilter = TriadicFilter(apply, positivePredicate, sourceId, targetId, Some(triSelection.id))(idGen)
      cardinalities.copy(from = triSelection.id, to = triFilter.id)
      providedOrders.copy(from = triSelection.id, to = triFilter.id)

      triFilter
  }, stopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
