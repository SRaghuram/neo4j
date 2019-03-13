/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.ast.ParameterFromSlot
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.util.Foldable._
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

/**
  * Rewrites a logical plan so that parameter access is done by offset into an array instead of accessing
  * a hash map.
  */
case object slottedParameters {

  def apply(input: LogicalPlan): (LogicalPlan, Map[String, Int]) = {
    val mapping: Map[String, Int] = input.treeFold(Set.empty[String]) {
      case Parameter(name, _) => acc => (acc + name, Some(identity))
    }.toArray.sorted.zipWithIndex.toMap

    val rewriter = bottomUp(Rewriter.lift {
      case Parameter(name, typ) => ParameterFromSlot(mapping(name), name, typ)
    })

    (input.endoRewrite(rewriter), mapping)
  }
}
