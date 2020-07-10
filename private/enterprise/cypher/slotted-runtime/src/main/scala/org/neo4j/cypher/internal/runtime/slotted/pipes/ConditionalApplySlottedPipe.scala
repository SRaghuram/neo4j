/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

case class ConditionalApplySlottedPipe(lhs: Pipe,
                                       rhs: Pipe,
                                       longOffsets: Seq[Int],
                                       refOffsets: Seq[Int],
                                       negated: Boolean,
                                       slots: SlotConfiguration,
                                       nullableSlots: Seq[Slot])
                                      (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setNullableSlotToNullFunctions =
  nullableSlots.map ({
    case LongSlot(offset, _, _) =>
      (context: CypherRow) => context.setLongAt(offset, -1L)
    case RefSlot(offset, _, _) =>
      (context: CypherRow) => context.setRefAt(offset, Values.NO_VALUE)
  })

  //===========================================================================
  // Runtime code
  //===========================================================================
  private def setNullableSlotsToNull(context: CypherRow): Unit = {
    val functions = setNullableSlotToNullFunctions.iterator
    while (functions.hasNext) {
      functions.next()(context)
    }
  }

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] =
    input.flatMap {
      lhsContext =>

        if (condition(lhsContext)) {
          val rhsState = state.withInitialContext(lhsContext)
          rhs.createResults(rhsState)
        }
        else {
          val output = SlottedRow(slots)
          setNullableSlotsToNull(output)
          output.copyAllFrom(lhsContext)
          ClosingIterator.single(output)
        }
    }

  private def condition(context: CypherRow): Boolean = {
    val hasNull =
      longOffsets.exists(offset => NullChecker.entityIsNull(context.getLongAt(offset))) ||
      refOffsets.exists(x => context.getRefAt(x) eq Values.NO_VALUE)
    if (negated) hasNull else !hasNull
  }

}
