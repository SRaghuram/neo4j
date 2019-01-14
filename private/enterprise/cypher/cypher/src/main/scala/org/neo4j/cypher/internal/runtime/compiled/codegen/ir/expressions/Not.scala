/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.v3_5.util.symbols.CTBoolean

case class Not(inner: CodeGenExpression) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    inner.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    if (!nullable) inner.codeGenType match {
      case t if t.isPrimitive => structure.notExpression (inner.generateExpression (structure) )
      case t => structure.unbox(structure.threeValuedNotExpression(structure.box(inner.generateExpression(structure), inner.codeGenType)),
                                CodeGenType.primitiveBool)
    }
    else structure.threeValuedNotExpression(structure.box(inner.generateExpression(structure), inner.codeGenType))

  override def nullable(implicit context: CodeGenContext) = inner.nullable

  override def codeGenType(implicit context: CodeGenContext) =
    if (!nullable) CodeGenType.primitiveBool
    else CypherCodeGenType(CTBoolean, ReferenceType)
}
