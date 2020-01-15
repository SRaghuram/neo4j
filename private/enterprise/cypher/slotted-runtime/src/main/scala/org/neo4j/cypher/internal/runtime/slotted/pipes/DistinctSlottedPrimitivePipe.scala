/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.impl.factory.Sets
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPrimitivePipe.buildGroupingValue
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrefetchingIterator}
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.{LongArray, Values}

case class DistinctSlottedPrimitivePipe(source: Pipe,
                                        slots: SlotConfiguration,
                                        primitiveSlots: Array[Int],
                                        groupingExpression: GroupingExpression)
                                       (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {
  groupingExpression.registerOwningPipe(this)


  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    new PrefetchingIterator[ExecutionContext] {
      private val seen = Sets.mutable.empty[LongArray]()

      override def produceNext(): Option[ExecutionContext] = {
        while (input.hasNext) {
          val next: ExecutionContext = input.next()

          val groupingValue = buildGroupingValue(next, primitiveSlots)
          if (seen.add(groupingValue)) {
            state.memoryTracker.allocated(groupingValue)
            // Found unseen key! Set it as the next element to yield, and exit
            val key = groupingExpression.computeGroupingKey(next, state)
            groupingExpression.project(next, key)
            return Some(next)
          }
        }
        None
      }
    }
  }
}

object DistinctSlottedPrimitivePipe {
  def buildGroupingValue(next: ExecutionContext, slots: Array[Int]): LongArray = {
    val keys = new Array[Long](slots.length)
    var i = 0
    while (i < slots.length) {
      keys(i) = next.getLongAt(slots(i))
      i += 1
    }
    Values.longArray(keys)
  }
}
