/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.functions

import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.{functions => astFunctions}
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

object functionConverter {

  def apply(fcn: ast.FunctionInvocation, callback: ast.Expression => CodeGenExpression)
           (implicit context: CodeGenContext): CodeGenExpression = fcn.function match {

    // id(n)
    case astFunctions.Id =>
      assert(fcn.args.size == 1)
      IdCodeGenFunction(callback(fcn.args(0)))

    // type(r)
    case astFunctions.Type =>
      assert(fcn.args.size == 1)
      TypeCodeGenFunction(callback(fcn.args(0)))

    case other => throw new CantCompileQueryException(s"Function $other not yet supported")
  }
}



