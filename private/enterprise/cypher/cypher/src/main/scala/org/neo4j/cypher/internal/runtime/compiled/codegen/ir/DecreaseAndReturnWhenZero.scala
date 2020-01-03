/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.codegen.Expression.{constant, invoke, newInstance}
import org.neo4j.codegen.MethodReference
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.{LessThan, LessThanEqual, MethodStructure}
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure.typeRef
import org.neo4j.exceptions.InvalidArgumentException

case class DecreaseAndReturnWhenZero(opName: String, variableName: String, action: Instruction, startValue: CodeGenExpression)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    startValue.init(generator)
    val expression = generator.box(startValue.generateExpression(generator), startValue.codeGenType)
    generator.declareCounter(variableName, expression, "LIMIT: Invalid input. Got a floating-point number. Must be a non-negative integer.")

    generator.ifStatement(generator.checkInteger(variableName, LessThan, 0L)) { onTrue =>
      val exception = invoke(newInstance(typeRef[InvalidArgumentException]),
        MethodReference.constructorReference(typeRef[InvalidArgumentException], typeRef[String], typeRef[Throwable]),
        constant("LIMIT: Invalid input. Got a negative integer. Must be a non-negative integer."), constant(null))
      onTrue.throwException(exception)
    }

    generator.ifStatement(generator.checkInteger(variableName, LessThanEqual, 0L)) { onTrue =>
      onTrue.returnSuccessfully()
    }
    action.init(generator)
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    action.body(generator)

    generator.trace(opName) { l1 =>
      l1.incrementRows()
      l1.decrementInteger(variableName)
      l1.ifStatement(l1.checkInteger(variableName, LessThanEqual, 0L)) { l2 =>
        l2.returnSuccessfully()
      }
    }
  }

  override protected def children: Seq[Instruction] = Seq(action)

  override protected def operatorId = Set(opName)
}
