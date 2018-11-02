/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v3_5.logical.plans.NestedPlanExpression
import org.opencypher.v9_0.expressions.functions.AggregatingFunction
import org.opencypher.v9_0.expressions.{functions, _}
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.{expressions => ast}

object MorselExpressionConverters extends ExpressionConverter {

  override def toCommandExpression(id: Id, expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    case c: FunctionInvocation if c.function == functions.Count =>
      Some(CountOperatorExpression(self.toCommandExpression(id, c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Avg =>
      Some(AvgOperatorExpression(self.toCommandExpression(id, c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Max =>
      Some(MaxOperatorExpression(self.toCommandExpression(id, c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Min =>
      Some(MinOperatorExpression(self.toCommandExpression(id, c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Collect =>
      Some(CollectOperatorExpression(self.toCommandExpression(id, c.arguments.head)))
    case _: CountStar => Some(CountStarOperatorExpression)

    //Queries containing these expression cant be handled by morsel runtime yet
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => throw new CantCompileQueryException()
    case e: NestedPlanExpression => throw new CantCompileQueryException(s"$e is not yet supported by the morsel runtime")

    case _ => None
  }

  override def toCommandProjection(id: Id, projections: Map[String, ast.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = None
}






