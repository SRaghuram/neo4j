/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.expressions

import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.{CartesianProduct, LogicalPlan, NestedPlanExpression, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.{File, Linenumber, Type}
import org.neo4j.exceptions.CantCompileQueryException

object PipelinedBlacklist {

  def throwOnUnsupportedPlan(logicalPlan: LogicalPlan, parallelExecution: Boolean, providedOrders: ProvidedOrders): Unit = {
    val unsupport =
      logicalPlan.fold(Set[String]()) {
        //Queries containing these expression cant be handled by morsel runtime yet
        case _: NestedPlanExpression =>
          _ + "Nested plan expressions"

        case _: ResolvedFunctionInvocation if parallelExecution =>
          _ + "User-defined functions"

        case f: FunctionInvocation if f.function == Linenumber || f.function == File =>
          _ + (f.functionName.name+"()")

        // type() uses thread-unsafe RelationshipEntity.type()
        case f: FunctionInvocation if f.function == Type && parallelExecution =>
          _ + (f.functionName.name+"()")

        case c: CartesianProduct if !providedOrders.getOrElse(c.left.id, ProvidedOrder.empty).isEmpty =>
          _ + "CartesianProduct if the LHS has a provided order"
      }
    if (unsupport.nonEmpty) {
      throw new CantCompileQueryException(s"Pipelined does not yet support ${unsupport.mkString("`", "`, `", "`")}, use another runtime.")
    }
  }
}
