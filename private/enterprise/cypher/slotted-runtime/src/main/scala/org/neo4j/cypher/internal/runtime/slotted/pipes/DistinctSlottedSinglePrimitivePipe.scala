/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.impl.factory.primitive.LongSets
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

case class DistinctSlottedSinglePrimitivePipe(source: Pipe,
                                              slots: SlotConfiguration,
                                              toSlot: Slot,
                                              offset: Int,
                                              expression: Expression)
                                             (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setInSlot: (CypherRow, AnyValue) => Unit = makeSetValueInSlotFunctionFor(toSlot)

  expression.registerOwningPipe(this)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[CypherRow],
                                      state: QueryState): Iterator[CypherRow] = {

    new PrefetchingIterator[CypherRow] {
      private val seen = LongSets.mutable.empty()

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) { // Let's pull data until we find something not already seen
          val next = input.next()
          val key = next.getLongAt(offset)
          if (seen.add(key)) {
            state.memoryTracker.allocated(java.lang.Long.BYTES, id.x)
            // Found something! Set it as the next element to yield, and exit
            val outputValue = expression(next, state)
            setInSlot(next, outputValue)
            return Some(next)
          }
        }

        None
      }
    }
  }
}
