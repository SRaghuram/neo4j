/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class Parameter(key: String, variableName: String, cType: CypherCodeGenType = CodeGenType.AnyValue) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.expectParameter(key, variableName, cType)

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E =
    structure.loadVariable(variableName)

  override def nullable(implicit context: CodeGenContext) = cType.canBeNullable

  override def codeGenType(implicit context: CodeGenContext) = cType
}
