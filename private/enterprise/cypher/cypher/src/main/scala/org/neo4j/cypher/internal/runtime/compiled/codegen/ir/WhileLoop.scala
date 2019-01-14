/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

case class WhileLoop(variable: Variable, producer: LoopDataGenerator, action: Instruction) extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    val iterator = s"${variable.name}Iter"
    generator.trace(producer.opName) { body =>
      producer.produceLoopData(iterator, body)
      body.whileLoop(producer.checkNext(body, iterator)) { loopBody =>
        loopBody.incrementRows()
        producer.getNext(variable, iterator, loopBody)
        action.body(loopBody)
      }
      producer.close(iterator, generator)
    }
  }

  override def operatorId: Set[String] = Set(producer.opName)

  override def children = Seq(action)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    producer.init(generator)
  }
}
