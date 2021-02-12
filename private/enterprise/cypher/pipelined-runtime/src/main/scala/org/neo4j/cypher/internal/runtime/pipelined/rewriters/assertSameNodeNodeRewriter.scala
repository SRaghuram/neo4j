/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertingMultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
  * Rewrite AssertSameNode into a specialized multiple index seek operator
  */
case class assertSameNodeNodeRewriter(stopper: AnyRef => Boolean) extends Rewriter {
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ AssertSameNode(node, lhs: NodeIndexSeekLeafPlan, rhs: NodeIndexSeekLeafPlan) =>
      AssertingMultiNodeIndexSeek(node, Array(lhs, rhs))(SameId(o.id))

    case o @ AssertSameNode(node, lhs: AssertingMultiNodeIndexSeek, rhs: NodeIndexSeekLeafPlan) if node == lhs.node && node == rhs.idName =>
      AssertingMultiNodeIndexSeek(node, lhs.nodeIndexSeeks :+ rhs)(SameId(o.id))

    case o @ AssertSameNode(node, lhs: NodeIndexSeekLeafPlan, rhs: AssertingMultiNodeIndexSeek) if node == rhs.node && node == lhs.idName =>
      AssertingMultiNodeIndexSeek(node, lhs +: rhs.nodeIndexSeeks)(SameId(o.id))

  }, stopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
