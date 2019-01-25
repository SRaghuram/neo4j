/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physical_planning.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class RollUpApplySlottedPipe(lhs: Pipe, rhs: Pipe,
                                  collectionRefSlotOffset: Int,
                                  identifierToCollect: (String, Expression),
                                  nullableIdentifiers: Set[String],
                                  slots: SlotConfiguration)
                                 (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) {

  private val getValueToCollectFunction = {
    val expression: Expression = identifierToCollect._2
    (state: QueryState) => (ctx: ExecutionContext) => expression(ctx, state)
  }

  private val hasNullValuePredicates: Seq[(ExecutionContext) => Boolean] =
    nullableIdentifiers.toSeq.map { elem =>
      val elemSlot = slots.get(elem)
      elemSlot match {
        case Some(LongSlot(offset, true, _)) => { (ctx: ExecutionContext) => ctx.getLongAt(offset) == -1 }
        case Some(RefSlot(offset, true, _)) => { (ctx: ExecutionContext) => ctx.getRefAt(offset) == NO_VALUE }
        case _ => { (ctx: ExecutionContext) => false }
      }
    }

  private def hasNullValue(ctx: ExecutionContext): Boolean =
    hasNullValuePredicates.exists(p => p(ctx))

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map {
      ctx =>
        if (hasNullValue(ctx)) {
          ctx.setRefAt(collectionRefSlotOffset, NO_VALUE)
        }
        else {
          val innerState = state.withInitialContext(ctx)
          val innerResults: Iterator[ExecutionContext] = rhs.createResults(innerState)
          val collection = VirtualValues.list(innerResults.map(getValueToCollectFunction(state)).toArray: _*)
          ctx.setRefAt(collectionRefSlotOffset, collection)
        }
        ctx
    }
  }
}
