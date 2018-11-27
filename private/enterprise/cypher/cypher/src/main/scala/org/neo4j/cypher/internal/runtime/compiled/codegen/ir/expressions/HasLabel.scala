/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.v3_5.util.symbols._

case class HasLabel(nodeVariable: Variable, labelVariable: String, labelName: String)
  extends CodeGenExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.lookupLabelId(labelVariable, labelName)

  def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    val localName = context.namer.newVarName()
    structure.declarePredicate(localName)

    structure.incrementDbHits()
    if (nodeVariable.nullable)
      structure.nullableReference(nodeVariable.name, CodeGenType.primitiveNode,
                                  structure.box(
                                    structure.hasLabel(nodeVariable.name, labelVariable, localName), CodeGenType.primitiveBool))
    else
      structure.hasLabel(nodeVariable.name, labelVariable, localName)
  }

  override def nullable(implicit context: CodeGenContext) = nodeVariable.nullable

  override def codeGenType(implicit context: CodeGenContext) =
    if (nullable) CypherCodeGenType(CTBoolean, ReferenceType) else CodeGenType.primitiveBool
}
