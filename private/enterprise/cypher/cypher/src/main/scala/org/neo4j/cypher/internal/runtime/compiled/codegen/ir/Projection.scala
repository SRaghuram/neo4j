/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

case class Projection(projectionOpName: String, variables: Map[Variable, CodeGenExpression], action: Instruction)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    super.init(generator)
    variables.foreach {
      case (_, expr) => expr.init(generator)
    }
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(projectionOpName) { body =>
      body.incrementRows()
      variables.foreach {
        case (variable, expr) =>
          body.declare(variable.name, variable.codeGenType)
          // Only materialize in produce results
          body.assign(variable.name, variable.codeGenType, expr.generateExpression(body))
      }
      action.body(body)
    }
  }

  override protected def operatorId = Set(projectionOpName)

  override protected def children = Seq(action)
}
