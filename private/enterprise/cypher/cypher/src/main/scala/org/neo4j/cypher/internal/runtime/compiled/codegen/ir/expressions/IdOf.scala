/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.v3_5.util.symbols

case class IdOf(variable: Variable) extends CodeGenExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {}

  def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E =
    if (nullable) structure.nullableReference(variable.name, variable.codeGenType,
                                              structure.box(structure.loadVariable(variable.name),
                                                            CypherCodeGenType(symbols.CTInteger, ReferenceType)))
    else structure.loadVariable(variable.name)

  override def nullable(implicit context: CodeGenContext): Boolean = variable.nullable

  override def codeGenType(implicit context: CodeGenContext): CypherCodeGenType =
    if (nullable) CypherCodeGenType(symbols.CTInteger, ReferenceType) else CodeGenType.primitiveInt
}
