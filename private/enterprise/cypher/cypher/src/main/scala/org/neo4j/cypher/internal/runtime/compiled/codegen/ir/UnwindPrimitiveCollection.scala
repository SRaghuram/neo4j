/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.{CodeGenExpression, CodeGenType, CypherCodeGenType, ListReferenceType}
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.opencypher.v9_0.util.symbols

case class UnwindPrimitiveCollection(opName: String, collection: CodeGenExpression) extends LoopDataGenerator {
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit =
    collection.init(generator)

  override def produceLoopData[E](iterVar: String, generator: MethodStructure[E])
                                 (implicit context: CodeGenContext): Unit = {
    generator.declarePrimitiveIterator(iterVar, collection.codeGenType)
    val iterator = generator.primitiveIteratorFrom(collection.generateExpression(generator), collection.codeGenType)
    generator.assign(iterVar, CodeGenType.Any, iterator)
  }

  override def getNext[E](nextVar: Variable, iterVar: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext): Unit = {
    val elementType = collection.codeGenType match {
      case CypherCodeGenType(symbols.ListType(innerCt), ListReferenceType(innerRepr)) => CypherCodeGenType(innerCt, innerRepr)
      case _ => throw new IllegalArgumentException(s"CodeGenType $collection.codeGenType not supported as primitive iterator")
    }
    val next = generator.primitiveIteratorNext(generator.loadVariable(iterVar), collection.codeGenType)
    generator.assign(nextVar.name, elementType, next)
  }

  override def checkNext[E](generator: MethodStructure[E], iterVar: String): E =
    generator.iteratorHasNext(generator.loadVariable(iterVar))

  override def close[E](iterVarName: String,
                        generator: MethodStructure[E]): Unit = {/*nothing to close*/}
}
