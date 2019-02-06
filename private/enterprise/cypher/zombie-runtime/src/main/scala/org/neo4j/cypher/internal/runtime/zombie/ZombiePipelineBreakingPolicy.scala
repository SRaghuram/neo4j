/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy
import org.neo4j.cypher.internal.v4_0.logical.plans._

object ZombiePipelineBreakingPolicy extends PipelineBreakingPolicy {

  override def breakOn(lp: LogicalPlan): Boolean = {

    lp match {
      // leaf operators
      case _: AllNodesScan |
           _: Argument // TODO: breaking on argument is often silly. Let's not do that when avoidable.
      => true

      // 1 child operators
      case _: Expand |
           _: Sort
      => true

      case _: ProduceResult
        => false

      // 2 child operators
      case _: Apply
      => true

      case _ =>
        throw new UnsupportedOperationException("not implemented")
    }
  }

  override def breakOnNestedPlan: Boolean = throw new UnsupportedOperationException("not implemented")
}
