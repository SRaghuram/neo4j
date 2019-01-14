/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.helpers.LiteralTypeSupport

case class Literal(value: Object) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    // When the literal value comes from the AST it should already have been converted
    assert({
      val needsConverison = value match {
        case n: java.lang.Byte => true // n.longValue()
        case n: java.lang.Short => true // n.longValue()
        case n: java.lang.Character => true // n.toString
        case n: java.lang.Integer => true // n.longValue()
        case n: java.lang.Float => true // n.doubleValue()
        case _ => false
      }
      !needsConverison
    })
    val ct = codeGenType
    if (value == null)
      structure.noValue()
    else if (ct.isPrimitive)
      structure.constantExpression(value)
    else
      structure.constantValueExpression(value, ct)
  }

  override def nullable(implicit context: CodeGenContext) = value == null

  override def codeGenType(implicit context: CodeGenContext) = LiteralTypeSupport.deriveCodeGenType(value)
}
