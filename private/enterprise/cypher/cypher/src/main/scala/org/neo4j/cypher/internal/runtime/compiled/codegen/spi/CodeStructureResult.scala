/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.spi

import org.neo4j.cypher.internal.plandescription.Argument

trait CodeStructureResult[T] {
  def query: T
  def code: Seq[Argument]
}
