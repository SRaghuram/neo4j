/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.functions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.cypher.internal

object functionConverter {

  def apply(fcn: internal.expressions.FunctionInvocation, callback: internal.expressions.Expression => CodeGenExpression)
           (implicit context: CodeGenContext): CodeGenExpression = fcn.function match {

    // id(n)
    case internal.expressions.functions.Id =>
      require(fcn.args.size == 1)
      IdCodeGenFunction(callback(fcn.args(0)))

    // type(r)
    case internal.expressions.functions.Type =>
      require(fcn.args.size == 1)
      TypeCodeGenFunction(callback(fcn.args(0)))

    case other => throw new CantCompileQueryException(s"Function $other not yet supported")
  }
}



