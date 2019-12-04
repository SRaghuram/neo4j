/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.macros.Require.require
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

case class RelationshipExpression(relId: Variable) extends CodeGenExpression {
  require(relId.codeGenType == CodeGenType.primitiveRel)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E =
    // Nullable primitive variables already have their nullValue (-1L) in the same domain as their possible values and do not need a null check
    structure.relationship(relId.name, relId.codeGenType)

  override def nullable(implicit context: CodeGenContext): Boolean = relId.nullable

  override def codeGenType(implicit context: CodeGenContext): CypherCodeGenType = CodeGenType.primitiveRel
}
