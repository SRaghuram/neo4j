/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.{OperatorFusionPolicy, PipelineBreakingPolicy}
import org.neo4j.exceptions.CantCompileQueryException

case class MorselPipelineBreakingPolicy(fusingPolicy: OperatorFusionPolicy) extends PipelineBreakingPolicy {

  override def breakOn(lp: LogicalPlan): Boolean = {

    def canFuseOneChildOperator: Boolean = fusingPolicy.canFuse(lp) && fusingPolicy.canFuse(lp.lhs.get)

    lp match {
      // leaf operators
      case _: AllNodesScan |
           _: NodeByLabelScan |
           _: NodeIndexSeek |
           _: NodeUniqueIndexSeek |
           _: NodeIndexContainsScan |
           _: NodeIndexEndsWithScan |
           _: NodeIndexScan |
           _: NodeByIdSeek |
           _: DirectedRelationshipByIdSeek |
           _: UndirectedRelationshipByIdSeek |
           _: NodeCountFromCountStore |
           _: RelationshipCountFromCountStore |
           _: Input |
           _: Argument // TODO: breaking on argument is often silly. Let's not do that when avoidable.
      => true

      // 1 child operators
      case e: Expand =>
        if (e.mode == ExpandAll) !canFuseOneChildOperator
        else
          throw unsupported("ExpandInto")
      case _: UnwindCollection |
           _: Sort |
           _: Top |
           _: Aggregation |
           _: Optional |
           _: VarExpand
    => !canFuseOneChildOperator

      case _: ProduceResult |
           _: Limit |
           _: Distinct |
           _: Projection |
           _: CacheProperties |
           _: Selection
        => false

      // 2 child operators
      case _: Apply |
           _: NodeHashJoin
      => true

      case plan =>
        throw unsupported(plan.getClass.getSimpleName)
    }
  }

  override def onNestedPlanBreak(): Unit = throw unsupported("NestedPlanExpression")

  private def unsupported(thing: String): CantCompileQueryException =
    new CantCompileQueryException(s"Morsel does not yet support the plans including `$thing`, use another runtime.")
}