/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class Multiplication(lhs: CodeGenExpression, rhs: CodeGenExpression)
  extends CodeGenExpression with BinaryOperator {

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = structure.multiplyExpression

  override def name: String = "multiply"
}
