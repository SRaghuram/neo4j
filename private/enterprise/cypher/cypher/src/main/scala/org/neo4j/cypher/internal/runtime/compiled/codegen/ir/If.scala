/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class If(predicate: CodeGenExpression, block: Instruction) extends Instruction {
  override protected def children: Seq[Instruction] = Seq(block)

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    generator.ifStatement(predicate.generateExpression(generator)) { inner =>
      inner.incrementRows()
      block.body(inner)
    }
  }

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    predicate.init(generator)
  }
}
