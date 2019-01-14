/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.{LessThan, MethodStructure}

case class SkipInstruction(opName: String, variableName: String, action: Instruction, numberToSkip: CodeGenExpression)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    numberToSkip.init(generator)
    val expression = generator.box(numberToSkip.generateExpression(generator), numberToSkip.codeGenType)
    generator.declareCounter(variableName, expression)
    action.init(generator)
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    generator.trace(opName) { l1 =>
      l1.incrementRows()
      l1.decrementInteger(variableName)
      l1.ifStatement(l1.checkInteger(variableName, LessThan, 0L)) { l2 =>
        action.body(l2)
      }
    }
  }

  override protected def children: Seq[Instruction] = Seq(action)

  override protected def operatorId = Set(opName)
}
