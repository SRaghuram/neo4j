/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen._
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.v4_0.util.InternalException

case class AnyProjection(variable: Variable) extends CodeGenExpression {
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) ={
    structure.materializeAny(structure.loadVariable(variable.name), codeGenType)
  }

  override def nullable(implicit context: CodeGenContext): Boolean = variable.nullable

  override def codeGenType(implicit context: CodeGenContext): CypherCodeGenType = variable.codeGenType match {
    case x: CypherCodeGenType => x
    case _ => throw new InternalException("Tried to create a Cypher value from a non-cypher-value variable")
  }

}
