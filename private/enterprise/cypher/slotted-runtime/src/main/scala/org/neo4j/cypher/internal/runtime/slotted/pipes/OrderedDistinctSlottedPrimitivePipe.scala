/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.function.Consumer

import org.eclipse.collections.impl.factory.Sets
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPrimitivePipe.buildGroupingValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.LongArray

case class OrderedDistinctSlottedPrimitivePipe(source: Pipe,
                                               slots: SlotConfiguration,
                                               primitiveSlots: Array[Int],
                                               orderedPrimitiveSlots: Array[Int],
                                               groupingExpression: GroupingExpression)
                                              (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[CypherRow],
                                      state: QueryState): Iterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private var seen = Sets.mutable.empty[LongArray]()
      private var currentOrderedGroupingValue: LongArray = _

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val groupingValue = buildGroupingValue(next, primitiveSlots)
          val orderedGroupingValue = buildGroupingValue(next, orderedPrimitiveSlots)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != orderedGroupingValue) {
            currentOrderedGroupingValue = orderedGroupingValue
            seen.forEach((x => state.memoryTracker.deallocated(x, id.x)) : Consumer[LongArray])
            seen = Sets.mutable.empty[LongArray]()
          }

          if (seen.add(groupingValue)) {
            state.memoryTracker.allocated(groupingValue, id.x)
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, groupingExpression.computeGroupingKey(next, state))
            return Some(next)
          }
        }
        None
      }
    }
  }
}
/**
 * Specialization for the case that all groupingColumns are ordered
 */
case class AllOrderedDistinctSlottedPrimitivePipe(source: Pipe,
                                                  slots: SlotConfiguration,
                                                  primitiveSlots: Array[Int],
                                                  groupingExpression: GroupingExpression)
                                                 (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[CypherRow],
                                      state: QueryState): Iterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private var currentOrderedGroupingValue: LongArray = _

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val groupingValue = buildGroupingValue(next, primitiveSlots)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != groupingValue) {
            currentOrderedGroupingValue = groupingValue
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, groupingExpression.computeGroupingKey(next, state))
            return Some(next)
          }
        }
        None
      }
    }
  }
}
