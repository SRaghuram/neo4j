/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime._
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.IndexReadSession

class ProjectOperator(val workIdentity: WorkIdentity,
                      val projectionOps: Map[Slot, Expression]) extends StatelessOperator {

  private val project = projectionOps.map {
    case (LongSlot(_, _, _),_) =>
      // We just pass along Long slot expressions without evaluation
      (_: ExecutionContext, _: OldQueryState) =>

    case (RefSlot(offset, _, _), expression) =>
      (ctx: ExecutionContext, state: OldQueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }.toArray

  override def operate(currentRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       cursors: ExpressionCursors): Unit = {

    val queryState = new OldQueryState(context, resources = null, params = state.params, cursors, Array.empty[IndexReadSession])

    while (currentRow.hasMoreRows) {
      project.foreach(p => p(currentRow, queryState))
      currentRow.moveToNextRow()
    }
  }
}
