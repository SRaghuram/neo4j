/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.Values

case class OptionalSlottedPipe(source: Pipe,
                               nullableSlots: Seq[Slot],
                               slots: SlotConfiguration,
                               argumentSize: SlotConfiguration.Size)
                              (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with Pipe {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setNullableSlotToNullFunctions =
    nullableSlots.map {
      case LongSlot(offset, _, _) =>
        (context: ExecutionContext) => context.setLongAt(offset, -1L)
      case RefSlot(offset, _, _) =>
        (context: ExecutionContext) => context.setRefAt(offset, Values.NO_VALUE)
    }

  //===========================================================================
  // Runtime code
  //===========================================================================
  private def setNullableSlotsToNull(context: ExecutionContext): Unit =
    setNullableSlotToNullFunctions.foreach { f =>
      f(context)
    }

  private def notFoundExecutionContext(state: QueryState): ExecutionContext = {
    val context = SlottedExecutionContext(slots)
    state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
    setNullableSlotsToNull(context)
    context
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    if (!input.hasNext) {
      Iterator(notFoundExecutionContext(state))
    } else {
      input
    }
}
