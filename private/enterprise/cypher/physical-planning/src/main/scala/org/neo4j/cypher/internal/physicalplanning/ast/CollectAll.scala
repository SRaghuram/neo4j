/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.plandescription.asPrettyString
import org.neo4j.cypher.internal.util.InputPosition

/**
  * Special aggregating function for performing collect() but preserving nulls. Not exposed in Cypher.
  * @param expr the expression to collect
  */
case class CollectAll(expr: Expression)(val position: InputPosition) extends Expression {
  override def asCanonicalStringVal: String = s"collect_all(${asPrettyString(expr)})"
}
