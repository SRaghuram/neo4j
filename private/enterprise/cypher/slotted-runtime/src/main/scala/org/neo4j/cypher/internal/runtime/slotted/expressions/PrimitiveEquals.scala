/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class PrimitiveEquals(a: Expression, b: Expression) extends Predicate with SlottedExpression {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val value1 = a(m, state)
    val value2 = b(m, state)
    Some(value1 == value2)
  }
  override def containsIsNull: Boolean = false

  override def rewrite(f: (Expression) => Expression): Expression = f(PrimitiveEquals(a.rewrite(f), b.rewrite(f)))
}
