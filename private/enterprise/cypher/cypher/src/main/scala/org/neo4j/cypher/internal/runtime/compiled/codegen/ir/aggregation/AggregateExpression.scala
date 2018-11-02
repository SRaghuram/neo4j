/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}


trait AggregateExpression {
  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def continuation(instruction: Instruction): Instruction = instruction
}
/**
  * Base class for aggregate expressions
  * @param expression the expression to aggregate
  * @param distinct is the aggregation distinct or not
  */
abstract class BaseAggregateExpression(expression: CodeGenExpression, distinct: Boolean) extends AggregateExpression {


  def distinctCondition[E](value: E, valueType: CodeGenType, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext)

  protected def ifNotNull[E](structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext) = {
    expression match {
      case NodeExpression(v) => primitiveIfNot(v, structure)(block(_))
      case RelationshipExpression(v) => primitiveIfNot(v, structure)(block(_))
      case expr =>
        val tmpName = context.namer.newVarName()
        structure.assign(tmpName, expression.codeGenType, expression.generateExpression(structure))
        val perhapsCheckForNotNullStatement: ((MethodStructure[E]) => Unit) => Unit = if (expr.nullable)
          structure.ifNonNullStatement(structure.loadVariable(tmpName), expression.codeGenType)
        else
          _(structure)

        perhapsCheckForNotNullStatement { body =>
          if (distinct) {
            distinctCondition(structure.loadVariable(tmpName), expression.codeGenType, body) { inner =>
              block(inner)
            }
          }
          else block(body)
        }
    }
  }

  private def primitiveIfNot[E](v: Variable, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                               (implicit context: CodeGenContext) = {
    structure.ifNotStatement(structure.equalityExpression(structure.loadVariable(v.name),
                                                          structure.constantExpression(Long.box(-1)),
                                                          CodeGenType.primitiveInt)) { body =>
      if (distinct) {
        distinctCondition(structure.loadVariable(v.name), CodeGenType.primitiveInt, body) { inner =>
          block(inner)
        }
      }
      else block(body)
    }
  }
}
