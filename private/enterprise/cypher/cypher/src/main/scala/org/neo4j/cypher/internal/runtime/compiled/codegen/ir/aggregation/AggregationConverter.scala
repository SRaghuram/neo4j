/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.ExpressionConverter._
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.{CodeGenExpression, CodeGenType}
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.opencypher.v9_0.expressions.{functions => astFunctions}
import org.opencypher.v9_0.{expressions => ast}

/*
* Conversion methods for aggregation functions
*/
object AggregationConverter {

  def aggregateExpressionConverter(opName: String, groupingVariables: Iterable[(String,CodeGenExpression)], name: String, e: ast.Expression) (implicit context: CodeGenContext): AggregateExpression = {
    val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveInt)
    context.addVariable(name, variable)
    context.addProjectedVariable(name, variable)
    e match {
      case func: ast.FunctionInvocation => func.function match {
        case astFunctions.Count if groupingVariables.isEmpty =>
          SimpleCount(variable, createExpression(func.args(0)), func.distinct)
        case astFunctions.Count  =>
          new DynamicCount(opName, variable, createExpression(func.args(0)), groupingVariables, func.distinct)

        case f => throw new CantCompileQueryException(s"$f is not supported")
      }
      case _ => throw new CantCompileQueryException(s"$e is not supported")
    }
  }
}
