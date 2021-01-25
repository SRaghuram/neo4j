/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

case class OptionalSlottedPipe(source: Pipe,
                               nullableSlots: Seq[Slot])
                              (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with Pipe {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setNullableSlotToNullFunctions =
  nullableSlots.map {
    case LongSlot(offset, _, _) =>
      (context: CypherRow) => context.setLongAt(offset, -1L)
    case RefSlot(offset, _, _) =>
      (context: CypherRow) => context.setRefAt(offset, Values.NO_VALUE)
  }

  //===========================================================================
  // Runtime code
  //===========================================================================
  private def setNullableSlotsToNull(context: CypherRow): Unit = {
    val functions = setNullableSlotToNullFunctions.iterator
    while (functions.hasNext) {
      functions.next()(context)
    }
  }

  private def notFoundExecutionContext(state: QueryState): CypherRow = {
    val context = state.newRowWithArgument(rowFactory)
    setNullableSlotsToNull(context)
    context
  }

  protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] =
    if (!input.hasNext) {
      ClosingIterator.single(notFoundExecutionContext(state))
    } else {
      input
    }
}
