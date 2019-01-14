/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

/**
 * Generates code that runs and afterwards checks if the provided variable has been set,
 * if not it sets all provided variables to null and runs the alternativeAction
 */
case class NullingInstruction(loop: Instruction, yieldedFlagVar: String, alternativeAction: Instruction,
                            nullableVars: Variable*)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    loop.init(generator)

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.declareFlag(yieldedFlagVar, initialValue = false)
    loop.body(generator)
    generator.ifNotStatement(generator.loadVariable(yieldedFlagVar)){ ifBody =>
      //mark variables as null
      nullableVars.foreach(v => ifBody.markAsNull(v.name, v.codeGenType))
      alternativeAction.body(ifBody)
    }
  }

  override def children = Seq(loop)
}
