/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.planLimitOnTopOf
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.plans.DropResult
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites DropResult into Limit(0).
 */
case class dropResultToLimitZero(cardinalities: Cardinalities,
                                 providedOrders: ProvidedOrders,
                                 idGen: IdGen,
                                 stopper: AnyRef => Boolean) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case dropResult @ DropResult(source) =>
      planLimitOnTopOf(source, SignedDecimalIntegerLiteral("0")(InputPosition.NONE))(SameId(dropResult.id))
  }, stopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
