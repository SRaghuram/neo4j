/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.impl.factory.Sets
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPrimitivePipe.buildGroupingValue
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrefetchingIterator}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.LongArray

case class OrderedDistinctSlottedPrimitivePipe(source: Pipe,
                                               slots: SlotConfiguration,
                                               primitiveSlots: Array[Int],
                                               orderedPrimitiveSlots: Array[Int],
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
      private var seen = Sets.mutable.empty[LongArray]()
      private var currentOrderedGroupingValue: LongArray = _

      override def produceNext(): Option[ExecutionContext] = {
        while (input.hasNext) {
          val next: ExecutionContext = input.next()

          val groupingValue = buildGroupingValue(next, primitiveSlots)
          val orderedGroupingValue = buildGroupingValue(next, orderedPrimitiveSlots)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != orderedGroupingValue) {
            currentOrderedGroupingValue = orderedGroupingValue
            seen = Sets.mutable.empty[LongArray]()
          }

          if (seen.add(groupingValue)) {
            // Found unseen key! Set it as the next element to yield, and exit
            val outgoing = SlottedExecutionContext(slots)
            outgoing.copyCachedFrom(next)
            outgoing.setLinenumber(next.getLinenumber)
            groupingExpression.project(outgoing, groupingExpression.computeGroupingKey(next, state))
            return Some(outgoing)
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
  groupingExpression.registerOwningPipe(this)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    new PrefetchingIterator[ExecutionContext] {
      private var currentOrderedGroupingValue: LongArray = _

      override def produceNext(): Option[ExecutionContext] = {
        while (input.hasNext) {
          val next: ExecutionContext = input.next()

          val groupingValue = buildGroupingValue(next, primitiveSlots)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != groupingValue) {
            currentOrderedGroupingValue = groupingValue
            // Found unseen key! Set it as the next element to yield, and exit
            val outgoing = SlottedExecutionContext(slots)
            outgoing.copyCachedFrom(next)
            outgoing.setLinenumber(next.getLinenumber)
            groupingExpression.project(outgoing, groupingExpression.computeGroupingKey(next, state))
            return Some(outgoing)
          }
        }
        None
      }
    }
  }
}
