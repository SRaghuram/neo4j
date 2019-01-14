/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

case class GetMatchesFromProbeTable(keys: Set[Variable], code: JoinData, action: Instruction) extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.trace(code.id, Some(this.getClass.getSimpleName)) { traced =>
      traced.probe(code.tableVar, code.tableType, keys.toIndexedSeq.map(_.name)) { body =>
        body.incrementRows()
        action.body(body)
      }
    }

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    action.init(generator)

  override def children = Seq(action)
}
