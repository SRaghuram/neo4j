/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.v4_0.util.symbols._

case class TypeOf(relId: Variable)
  extends CodeGenExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    val typeName = context.namer.newVarName()
    structure.declareAndInitialize(typeName, CypherCodeGenType(CTString, ReferenceType))
    if (nullable) {
      structure.ifNotStatement(structure.isNull(relId.name, CodeGenType.primitiveRel)) { body =>
        body.relType(relId.name, typeName)
      }
      structure.loadVariable(typeName)
    }
    else {
      structure.relType(relId.name, typeName)
      structure.loadVariable(typeName)
    }
  }

  override def nullable(implicit context: CodeGenContext) = relId.nullable

  override def codeGenType(implicit context: CodeGenContext) = CypherCodeGenType(CTString, ReferenceType)
}
