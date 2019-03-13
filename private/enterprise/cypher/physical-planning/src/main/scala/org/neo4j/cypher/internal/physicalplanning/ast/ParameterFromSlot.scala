/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType

case class ParameterFromSlot(offset: Int, name: String, parameterType: CypherType) extends RuntimeExpression {
  override def asCanonicalStringVal: String = s"{$name}"
}
