/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.spi

import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{ByteCode, SourceCode}

trait CodeStructureResult[T] {
  def query: T
  def code: Seq[Argument] = {
    source.map {
      case (className, sourceCode) => SourceCode(className, sourceCode)
    } ++ bytecode.map {
      case (className, byteCode) => ByteCode(className, byteCode)
    }
  }
  def source: Seq[(String, String)]
  def bytecode: Seq[(String, String)]
}
