/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans.{NestedPlanExpression, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, GroupingExpression}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

object MorselExpressionConverters extends ExpressionConverter {

  override def toCommandExpression(id: Id, expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    //Queries containing these expression cant be handled by morsel runtime yet
    case e: NestedPlanExpression => throw new CantCompileQueryException(s"$e is not yet supported by the morsel runtime")
    case _: ResolvedFunctionInvocation => throw new CantCompileQueryException(s"User-defined functions are not yet supported by the morsel runtime")

    case _ => None
  }

  override def toCommandProjection(id: Id, projections: Map[String, ast.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = None

  override def toGroupingExpression(id: Id,
                                    groupings: Map[String, ast.Expression],
                                    orderToLeverage: Seq[ast.Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = None
}
