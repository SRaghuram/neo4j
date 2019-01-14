/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen._
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class MethodInvocation(override val operatorId: Set[String],
                            symbol:JoinTableMethod,
                            methodName: String,
                            statements: Seq[Instruction]) extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.invokeMethod(symbol.tableType, symbol.name, methodName) { body =>
      statements.foreach(_.init(body))
      statements.foreach(_.body(body))
    }
  }

  override def children = statements
}
