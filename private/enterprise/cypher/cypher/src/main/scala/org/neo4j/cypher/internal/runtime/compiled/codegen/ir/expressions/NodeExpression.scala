/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

case class NodeExpression(nodeIdVar: Variable) extends CodeGenExpression {

  assert(nodeIdVar.codeGenType == CodeGenType.primitiveNode || nodeIdVar.codeGenType == CodeGenType.Any)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    // Nullable primitive variables already have their nullValue (-1L) in the same domain as their possible values and do not need a null check
    structure.node(nodeIdVar.name, nodeIdVar.codeGenType)

  override def nullable(implicit context: CodeGenContext) = nodeIdVar.nullable

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType.primitiveNode // MethodStructure.node() always returns a primitive node
}
