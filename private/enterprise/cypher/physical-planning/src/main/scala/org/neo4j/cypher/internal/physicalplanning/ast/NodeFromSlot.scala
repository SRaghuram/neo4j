/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.runtime.ast.RuntimeVariable

case class NodeFromSlot(offset: Int, override val name: String) extends RuntimeVariable(name) {
  override def asCanonicalStringVal: String = name
}
