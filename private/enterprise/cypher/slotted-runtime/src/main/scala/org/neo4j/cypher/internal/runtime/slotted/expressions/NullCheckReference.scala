/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE

case class NullCheckReference(offset: Int, inner: Expression) extends Expression with SlottedExpression {

  override def apply(ctx: ReadableRow, state: QueryState): AnyValue =
    if (ctx.getRefAt(offset) eq NO_VALUE) NO_VALUE else inner(ctx, state)

  override def children: Seq[AstNode[_]] = Seq(inner)
}