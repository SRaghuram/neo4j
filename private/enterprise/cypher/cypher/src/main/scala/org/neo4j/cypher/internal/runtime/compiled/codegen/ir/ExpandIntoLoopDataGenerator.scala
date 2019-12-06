/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.expressions.SemanticDirection

case class ExpandIntoLoopDataGenerator(opName: String,
                                       fromVar: Variable,
                                       dir: SemanticDirection,
                                       types: Map[String, String],
                                       toVar: Variable,
                                       relVar: Variable,
                                       expandIntoVar: String)
  extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    types.foreach {
      case (typeVar,relType) => generator.lookupRelationshipTypeId(typeVar, relType)
    }
    generator.createCachingExpandInto(expandIntoVar, dir)
  }

  override def produceLoopData[E](cursorName: String, generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
      generator.connectingRelationships(cursorName,
                                        expandIntoVar,
                                        fromVar.name,
                                        fromVar.codeGenType,
                                        toVar.name,
                                        toVar.codeGenType,
                                        types.keys.toIndexedSeq)
    generator.incrementDbHits()
  }

  override def getNext[E](nextVar: Variable, cursorName: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext): Unit = {
    generator.incrementDbHits()
    generator.nextRelationship(cursorName, dir, relVar.name)
  }

  override def checkNext[E](generator: MethodStructure[E], cursorName: String): E =
    generator.advanceRelationshipSelectionCursor(cursorName)

  override def close[E](cursorName: String, generator: MethodStructure[E]): Unit =
    generator.closeRelationshipSelectionCursor(cursorName)
}
