/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.expressions

import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.exceptions.CantCompileQueryException

object PipelinedBlacklist {

  def throwOnUnsupportedPlan(logicalPlan: LogicalPlan, parallelExecution: Boolean, leveragedOrders: LeveragedOrders): Unit = {
    val unsupport =
      logicalPlan.fold(Set[String]()) {
        //Queries containing these expression cant be handled by morsel runtime yet
        case _: NestedPlanExpression =>
          _ + "Nested plan expressions"

        case _: ResolvedFunctionInvocation if parallelExecution =>
          _ + "User-defined functions"

        case f: FunctionInvocation if f.function == functions.Linenumber || f.function == functions.File =>
          _ + (f.functionName.name+"()")

        // type() uses thread-unsafe RelationshipEntity.type()
        case f: FunctionInvocation if f.function == functions.Type && parallelExecution =>
          _ + (f.functionName.name+"()")

        case c: CartesianProduct if leveragedOrders.get(c.id) =>
          _ + "CartesianProduct if it needs to maintain order"
      }
    if (unsupport.nonEmpty) {
      throw new CantCompileQueryException(s"Pipelined does not yet support ${unsupport.mkString("`", "`, `", "`")}, use another runtime.")
    }
  }
}
