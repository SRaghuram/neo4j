/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

case class ForEachExpression(varName: Variable, expression: CodeGenExpression, body: Instruction) extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    body.init(generator)
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =

    generator.forEach(varName.name, varName.codeGenType, expression.generateExpression(generator)) { forBody =>
      body.body(forBody)
    }

  override def children = Seq(body)
}
