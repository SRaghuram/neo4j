/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class PrimitiveEquals(a: Expression, b: Expression) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    val value1 = a(ctx, state)
    val value2 = b(ctx, state)
    Some(value1 == value2)
  }
  override def containsIsNull: Boolean = false

  override def rewrite(f: Expression => Expression): Expression = f(PrimitiveEquals(a.rewrite(f), b.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(a, b)
}
