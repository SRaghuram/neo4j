/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class MapProperty(mapExpression: CodeGenExpression, propertyKeyName: String) extends CodeGenExpression {
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    mapExpression.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E =
    structure.mapGetExpression(mapExpression.generateExpression(structure), propertyKeyName)

  override def nullable(implicit context: CodeGenContext) = true

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType.Any
}
