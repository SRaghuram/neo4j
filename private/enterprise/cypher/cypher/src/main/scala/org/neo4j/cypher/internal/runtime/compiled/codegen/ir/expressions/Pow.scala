/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.v3_5.util.symbols.CTFloat

case class Pow(lhs: CodeGenExpression, rhs: CodeGenExpression)
  extends CodeGenExpression with BinaryOperator {

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = structure.powExpression

  override def name: String = "pow"

  override def codeGenType(implicit context: CodeGenContext) =
    (lhs.codeGenType.ct, rhs.codeGenType.ct) match {
      case (Number(_), Number(_)) => CypherCodeGenType(CTFloat, ReferenceType)
      case _ => CodeGenType.Any
    }
}
