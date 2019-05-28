/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy

object MorselPipelineBreakingPolicy extends PipelineBreakingPolicy {

  override def breakOn(lp: LogicalPlan): Boolean = {

    lp match {
      // leaf operators
      case _: AllNodesScan |
           _: NodeByLabelScan |
           _: NodeIndexSeek |
           _: NodeUniqueIndexSeek |
           _: NodeIndexContainsScan |
           _: NodeIndexScan |
           _: Input |
           _: Argument // TODO: breaking on argument is often silly. Let's not do that when avoidable.
      => true

      // 1 child operators
      case e: Expand if e.mode == ExpandAll
        => true
      case _: UnwindCollection |
           _: Sort |
           _: Aggregation
      => true

      case _: ProduceResult |
           _: Limit |
           _: Projection |
           _: Selection
        => false

      // 2 child operators
      case _: Apply |
           _: NodeHashJoin
      => true

      case plan =>
        throw new UnsupportedOperationException(s"Morsel does not yet support the planned operator `${plan.getClass.getSimpleName}`, use another runtime.")
    }
  }

  override def onNestedPlanBreak(): Unit = throw new UnsupportedOperationException("not implemented")
}
