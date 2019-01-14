/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

trait CodeGenExpression {
  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit
  def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E
  def nullable(implicit context: CodeGenContext): Boolean
  def codeGenType(implicit context: CodeGenContext): CypherCodeGenType

  def needsJavaNullCheck(implicit context: CodeGenContext): Boolean = {
    this.nullable && this.codeGenType.repr == ReferenceType
  }
}
