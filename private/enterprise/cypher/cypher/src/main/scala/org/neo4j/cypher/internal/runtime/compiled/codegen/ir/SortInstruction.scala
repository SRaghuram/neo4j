/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

case class SortInstruction(opName: String,
                           sortTableInfo: SortTableInfo)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(opName, Some(this.getClass.getSimpleName)) { body =>
      body.sortTableSort(sortTableInfo.tableName, sortTableInfo.tableDescriptor)
    }
  }

  override protected def children: Seq[Instruction] = Seq.empty

  override protected def operatorId = Set(opName)
}
