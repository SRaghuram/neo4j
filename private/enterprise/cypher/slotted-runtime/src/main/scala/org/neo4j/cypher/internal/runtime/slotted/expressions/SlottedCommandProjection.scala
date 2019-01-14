/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, ExecutionContext}

case class SlottedCommandProjection(introducedExpressions: Map[Int, Expression]) extends CommandProjection {

  override def isEmpty: Boolean = introducedExpressions.isEmpty

  override def registerOwningPipe(pipe: Pipe): Unit = introducedExpressions.values.foreach(_.registerOwningPipe(pipe))

  private val projectionFunctions: Iterable[(ExecutionContext, QueryState) => Unit] = introducedExpressions map {
    case (offset, expression) =>
      (ctx: ExecutionContext, state: QueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }

  override def project(ctx: ExecutionContext, state: QueryState): Unit = projectionFunctions.foreach(_ (ctx, state))
}
