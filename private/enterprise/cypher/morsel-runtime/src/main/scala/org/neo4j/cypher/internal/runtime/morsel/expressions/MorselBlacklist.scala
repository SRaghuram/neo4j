/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, NestedPlanExpression, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.v4_0.expressions.FunctionInvocation
import org.neo4j.cypher.internal.v4_0.expressions.functions.{Filename, Linenumber, Type}

object MorselBlacklist {

  private val BLACKLISTED_FUNCTIONS = Seq(Linenumber,
                                          Filename,
                                          Type) // type() uses thread-unsafe RelationshipProxy.type()

  def throwOnUnsupportedPlan(logicalPlan: LogicalPlan): Unit = {
    val unsupport =
      logicalPlan.fold(Set[String]()) {
        //Queries containing these expression cant be handled by morsel runtime yet
        case _: NestedPlanExpression =>
          _ + "Nested plan expressions"

        case _: ResolvedFunctionInvocation =>
          _ + "User-defined functions"

        case f: FunctionInvocation if BLACKLISTED_FUNCTIONS.contains(f.function) =>
          _ + (f.functionName.name+"()")
      }
    if (unsupport.nonEmpty) {
      throw new CantCompileQueryException(s"Morsel does not yet support ${unsupport.mkString("`", "`, `", "`")}, use another runtime.")
    }
  }
}
