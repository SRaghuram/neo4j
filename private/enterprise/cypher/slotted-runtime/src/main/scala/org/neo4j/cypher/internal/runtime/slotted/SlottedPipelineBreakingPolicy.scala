/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy
import org.neo4j.cypher.internal.util.attribution.Id

object SlottedPipelineBreakingPolicy extends PipelineBreakingPolicy {

  override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = {

    lp match {
      // leaf operators
      case _: LogicalLeafPlan
      => true

      // 1 child operators
      case _: Aggregation |
           _: Expand |
           _: OptionalExpand |
           _: VarExpand |
           _: PruningVarExpand |
           _: UnwindCollection |
           _: Eager
        // _: ProjectEndpoints | This is cardinality increasing (if undirected) but doesn't break currently
        // _: LoadCSV | This is cardinality increasing but doesn't break currently
        // _: ProcedureCall | This is cardinality increasing but doesn't break currently
        //                    Also, if the procedure is void it cannot increase cardinality.
        // _: FindShortestPaths | This is cardinality increasing but doesn't break currently
      => true

      // 2 child operators
      case _: CartesianProduct |
           _: RightOuterHashJoin |
           _: LeftOuterHashJoin |
           _: NodeHashJoin |
           _: ValueHashJoin |
           _: Union
      => true

      case _ =>
        false
    }
  }
}
