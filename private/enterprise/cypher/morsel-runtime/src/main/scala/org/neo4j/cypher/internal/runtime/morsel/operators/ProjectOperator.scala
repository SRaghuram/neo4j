/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
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
                       resources: QueryResources): Unit = {

    val queryState = new OldQueryState(context,
                                       resources = null,
                                       parameterArray = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession],
                                       resources.expressionVariables(state.nExpressionSlots))

    while (currentRow.isValidRow) {
      project.foreach(p => p(currentRow, queryState))
      currentRow.moveToNextRow()
    }
  }
}
