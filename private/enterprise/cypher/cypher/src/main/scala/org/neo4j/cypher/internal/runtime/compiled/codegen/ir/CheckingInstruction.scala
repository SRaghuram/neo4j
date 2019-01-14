/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

/**
 * Generates instruction for for updating a provided flag before creating the inner instruction
 */
case class CheckingInstruction(inner: Instruction, yieldedFlagVar: String)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    inner.init(generator)
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.updateFlag(yieldedFlagVar, newValue = true)
    inner.body(generator)
  }

  override def children = Seq(inner)
}
