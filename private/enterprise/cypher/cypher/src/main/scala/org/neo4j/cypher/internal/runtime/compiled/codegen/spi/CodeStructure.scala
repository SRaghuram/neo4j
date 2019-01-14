/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.spi

import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenConfiguration, CodeGenContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
 * This constitutes the SPI for code generation.
 */
trait CodeStructure[T] {
  def generateQuery(className: String, columns: Seq[String], operatorIds: Map[String, Id], conf: CodeGenConfiguration)
                   (block: MethodStructure[_] => Unit)(implicit codeGenContext: CodeGenContext): CodeStructureResult[T]
}
