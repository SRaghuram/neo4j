/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class SlottedCommandProjection(introducedExpressions: Map[Int, Expression]) extends CommandProjection {

  override def isEmpty: Boolean = introducedExpressions.isEmpty

  private val projectionFunctions: Iterable[(ReadWriteRow, QueryState) => Unit] = introducedExpressions map {
    case (offset, expression) =>
      (ctx: ReadWriteRow, state: QueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }

  override def project(ctx: ReadWriteRow, state: QueryState): Unit = projectionFunctions.foreach(_ (ctx, state))
}
