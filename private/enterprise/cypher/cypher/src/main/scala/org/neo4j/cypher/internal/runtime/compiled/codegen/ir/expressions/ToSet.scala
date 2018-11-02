/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class ToSet(expression: CodeGenExpression) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = expression.init(generator)

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    structure.toSet(expression.generateExpression(structure))

  override def nullable(implicit context: CodeGenContext) = false

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType.Any
}
