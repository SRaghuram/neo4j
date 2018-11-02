/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.opencypher.v9_0.util.symbols._

case class RelationshipProjection(relId: Variable) extends CodeGenExpression {
  assert(relId.codeGenType.asInstanceOf[CypherCodeGenType].ct == CTRelationship)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) ={
    if (relId.nullable)
      structure.nullableReference(relId.name, CodeGenType.primitiveRel,
        structure.materializeRelationship(relId.name, relId.codeGenType))
    else
      structure.materializeRelationship(relId.name, relId.codeGenType)
  }

  override def nullable(implicit context: CodeGenContext) = relId.nullable

  override def codeGenType(implicit context: CodeGenContext) = CypherCodeGenType(CTRelationship, ReferenceType)
}
