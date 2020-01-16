/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.Variable
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.ExpressionConverter.createExpression
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.cypher.internal

/*
* Conversion methods for aggregation functions
*/
object AggregationConverter {

  def aggregateExpressionConverter(opName: String, groupingVariables: Iterable[(String,CodeGenExpression)], name: String, e: internal.expressions.Expression) (implicit context: CodeGenContext): AggregateExpression = {
    val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveInt)
    context.addVariable(name, variable)
    context.addProjectedVariable(name, variable)
    e match {
      case func: internal.expressions.FunctionInvocation => func.function match {
        case internal.expressions.functions.Count if groupingVariables.isEmpty =>
          SimpleCount(opName, variable, createExpression(func.args(0)), func.distinct)
        case internal.expressions.functions.Count  =>
          new DynamicCount(opName, variable, createExpression(func.args(0)), groupingVariables, func.distinct)

        case f => throw new CantCompileQueryException(s"$f is not supported")
      }
      case _ => throw new CantCompileQueryException(s"$e is not supported")
    }
  }
}
