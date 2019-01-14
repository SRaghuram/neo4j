/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.v4_0.util.symbols._

case class NodeProjection(nodeIdVar: Variable) extends CodeGenExpression {

  assert(nodeIdVar.codeGenType.asInstanceOf[CypherCodeGenType].ct == CTNode)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) ={
    if (nodeIdVar.nullable)
      structure.nullableReference(nodeIdVar.name, CodeGenType.primitiveNode,
        structure.materializeNode(nodeIdVar.name, nodeIdVar.codeGenType))
    else
      structure.materializeNode(nodeIdVar.name, nodeIdVar.codeGenType)
  }

  override def nullable(implicit context: CodeGenContext) = nodeIdVar.nullable

  override def codeGenType(implicit context: CodeGenContext) = CypherCodeGenType(CTNode, ReferenceType)
}
