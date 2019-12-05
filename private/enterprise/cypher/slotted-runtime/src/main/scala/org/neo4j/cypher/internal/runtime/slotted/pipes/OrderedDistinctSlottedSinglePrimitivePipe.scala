/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrefetchingIterator}
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

case class OrderedDistinctSlottedSinglePrimitivePipe(source: Pipe,
                                                     slots: SlotConfiguration,
                                                     toSlot: Slot,
                                                     offset: Int,
                                                     expression: Expression)
                                                    (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setInSlot: (ExecutionContext, AnyValue) => Unit = makeSetValueInSlotFunctionFor(toSlot)

  expression.registerOwningPipe(this)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    new PrefetchingIterator[ExecutionContext] {
      private var currentOrderedGroupingValue: Long = -1

      override def produceNext(): Option[ExecutionContext] = {
        while (input.hasNext) { // Let's pull data until we find something not already seen
          val next = input.next()
          val groupingValue = next.getLongAt(offset)
          if (currentOrderedGroupingValue == -1 || currentOrderedGroupingValue != groupingValue) {
            currentOrderedGroupingValue = groupingValue
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
