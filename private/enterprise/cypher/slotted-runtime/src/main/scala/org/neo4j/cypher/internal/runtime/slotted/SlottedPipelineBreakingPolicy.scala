/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy
import org.neo4j.cypher.internal.v4_0.logical.plans._

object SlottedPipelineBreakingPolicy extends PipelineBreakingPolicy {

  override def breakOn(lp: LogicalPlan): Boolean = {

    lp match {
        // leaf operators
      case _: LogicalLeafPlan
        => true

        // 1 child operators
      case _: Distinct |
           _: Aggregation |
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

  override def breakOnNestedPlan: Boolean = true
}
