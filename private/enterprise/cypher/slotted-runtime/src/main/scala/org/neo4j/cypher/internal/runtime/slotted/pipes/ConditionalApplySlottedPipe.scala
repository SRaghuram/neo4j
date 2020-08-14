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
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

abstract class BaseConditionalApplySlottedPipe(lhs: Pipe,
                                       rhs: Pipe,
                                       longOffsets: Seq[Int],
                                       refOffsets: Seq[Int],
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

  def condition(context: CypherRow): Boolean
}

case class ConditionalApplySlottedPipe(lhs: Pipe,
                                       rhs: Pipe,
                                       longOffsets: Array[Int],
                                       refOffsets: Array[Int],
                                       slots: SlotConfiguration,
                                       nullableSlots: Seq[Slot])
                                       (id: Id = Id.INVALID_ID)
  extends BaseConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, slots, nullableSlots)(id) {
  override def condition(context: CypherRow): Boolean = {
    var i = 0
    while (i < longOffsets.length) {
      if (entityIsNull(context.getLongAt(longOffsets(i)))) {
        return false
      }
      i += 1
    }
    i = 0
    while (i < refOffsets.length) {
      if (context.getRefAt(refOffsets(i)) eq Values.NO_VALUE) {
        return false
      }
      i += 1
    }
    true
  }
}

case class AntiConditionalApplySlottedPipe(lhs: Pipe,
                                           rhs: Pipe,
                                           longOffsets: Array[Int],
                                           refOffsets: Array[Int],
                                           slots: SlotConfiguration,
                                           nullableSlots: Seq[Slot])
                                          (id: Id = Id.INVALID_ID)
  extends BaseConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, slots, nullableSlots)(id) {
  override def condition(context: CypherRow): Boolean = {
    var i = 0
    while (i < longOffsets.length) {
      if (!entityIsNull(context.getLongAt(longOffsets(i)))) {
        return false
      }
      i += 1
    }
    i = 0
    while (i < refOffsets.length) {
      if (!(context.getRefAt(refOffsets(i)) eq Values.NO_VALUE)) {
        return false
      }
      i += 1
    }
    true
  }
}
