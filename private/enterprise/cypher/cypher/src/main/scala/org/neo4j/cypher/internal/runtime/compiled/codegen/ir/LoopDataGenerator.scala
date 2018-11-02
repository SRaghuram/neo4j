/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

// Generates the code that moves data into local variables from the iterator or cursor being consumed.
trait LoopDataGenerator {

  def checkNext[E](generator: MethodStructure[E], iterVar: String): E

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def getNext[E](nextVar: Variable, iterVar: String, generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def produceLoopData[E](iterVarName: String, generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def close[E](iterVarName: String, generator: MethodStructure[E]): Unit

  def opName: String
}
