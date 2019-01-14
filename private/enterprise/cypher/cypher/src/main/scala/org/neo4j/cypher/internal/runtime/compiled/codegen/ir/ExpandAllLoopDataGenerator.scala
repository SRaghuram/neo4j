/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection

case class ExpandAllLoopDataGenerator(opName: String, fromVar: Variable, dir: SemanticDirection,
                   types: Map[String, String], toVar: Variable, relVar: Variable)
  extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    types.foreach {
      case (typeVar,relType) => generator.lookupRelationshipTypeId(typeVar, relType)
    }
  }

  override def produceLoopData[E](cursorName: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    if(types.isEmpty)
      generator.nodeGetRelationshipsWithDirection(cursorName, fromVar.name, fromVar.codeGenType, dir)
    else
      generator.nodeGetRelationshipsWithDirectionAndTypes(cursorName, fromVar.name, fromVar.codeGenType, dir, types.keys.toIndexedSeq)
    generator.incrementDbHits()
  }

  override def getNext[E](nextVar: Variable, cursorName: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext) = {
    generator.incrementDbHits()
    generator.nextRelationshipAndNode(toVar.name, cursorName, dir, fromVar.name, relVar.name)
  }

  override def checkNext[E](generator: MethodStructure[E], cursorName: String): E =  generator.advanceRelationshipSelectionCursor(cursorName)

  override def close[E](cursorName: String,
                        generator: MethodStructure[E]): Unit = generator.closeRelationshipSelectionCursor(cursorName)
}
