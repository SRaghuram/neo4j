/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class AcceptVisitor(produceResultOpName: String, columns: Map[String, CodeGenExpression])
  extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(produceResultOpName) { body =>
      body.incrementRows()
      columns.foreach { case (k, v: CodeGenExpression) =>
        body.setInRow(context.nameToIndex(k), anyValue(body, v))
      }
      body.visitorAccept()
    }
  }

  private def anyValue[E](generator: MethodStructure[E], v: CodeGenExpression)(implicit context: CodeGenContext) = {
    if (v.needsJavaNullCheck) {
      val variable = context.namer.newVarName()
      generator.localVariable(variable, v.generateExpression(generator), v.codeGenType)
      generator.ternaryOperator(generator.isNull(generator.loadVariable(variable), v.codeGenType),
                                generator.noValue(),
                                generator.toMaterializedAnyValue(generator.loadVariable(variable), v.codeGenType))
    }
    else generator.toMaterializedAnyValue(v.generateExpression(generator), v.codeGenType)
  }

  override protected def operatorId = Set(produceResultOpName)

  override protected def children = Seq.empty

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) {
    columns.values.foreach(_.init(generator))
    super.init(generator)
  }
}
