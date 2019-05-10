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
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrefetchingIterator}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.AnyValue

case class OrderedDistinctSlottedPipe(source: Pipe,
                                      slots: SlotConfiguration,
                                      groupingExpression: GroupingExpression)
                                     (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  groupingExpression.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    new PrefetchingIterator[ExecutionContext] {
      private var seen = Sets.mutable.empty[AnyValue]()
      private var currentOrderedGroupingValue: AnyValue = _

      override def produceNext(): Option[ExecutionContext] = {
        while (input.hasNext) {
          val next: ExecutionContext = input.next()

          val groupingValue = groupingExpression.computeGroupingKey(next, state)
          val orderedGroupingValue = groupingExpression.computeOrderedGroupingKey(groupingValue)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != orderedGroupingValue) {
            currentOrderedGroupingValue = orderedGroupingValue
            seen = Sets.mutable.empty[AnyValue]()
          }

          if (seen.add(groupingValue)) {
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, groupingValue)
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
case class AllOrderedDistinctSlottedPipe(source: Pipe,
                                      slots: SlotConfiguration,
                                      groupingExpression: GroupingExpression)
                                     (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  groupingExpression.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    new PrefetchingIterator[ExecutionContext] {
      private var currentOrderedGroupingValue: AnyValue = _

      override def produceNext(): Option[ExecutionContext] = {
        while (input.hasNext) {
          val next: ExecutionContext = input.next()

          val groupingValue = groupingExpression.computeGroupingKey(next, state)

          if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != groupingValue) {
            currentOrderedGroupingValue = groupingValue
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, groupingValue)
            return Some(next)
          }
        }
        None
      }
    }
  }
}
