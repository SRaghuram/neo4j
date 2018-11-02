/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.aggregation.AggregateExpression
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class AggregationInstruction(opName: String, aggregationFunctions: Iterable[AggregateExpression])
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) {
    aggregationFunctions.foreach(_.init(generator))
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(opName) { l1 =>
      aggregationFunctions.foreach(_.update(l1))
    }
  }

  override protected def children: Seq[Instruction] = Seq.empty

  override protected def operatorId = Set(opName)
}
