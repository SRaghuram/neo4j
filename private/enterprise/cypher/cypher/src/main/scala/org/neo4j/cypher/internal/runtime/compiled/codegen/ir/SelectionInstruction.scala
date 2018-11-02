/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class SelectionInstruction(id: String, instruction: Instruction) extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = super.init(generator)

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.trace(id) { body =>
      instruction.body(body)
    }

  override def children = Seq(instruction)

  override protected def operatorId = Set(id)
}
