/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

case class ScanAllNodes(opName: String) extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def produceLoopData[E](cursorName: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.allNodesScan(cursorName)
    generator.incrementDbHits()
  }

  override def getNext[E](nextVar: Variable, cursorName: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext) = {
    generator.incrementDbHits()
    generator.nodeFromNodeCursor(nextVar.name, cursorName)
  }

  override def checkNext[E](generator: MethodStructure[E], cursorName: String): E = generator.advanceNodeCursor(cursorName)

  override def close[E](cursorName: String, generator: MethodStructure[E]): Unit = generator.closeNodeCursor(cursorName)
}
