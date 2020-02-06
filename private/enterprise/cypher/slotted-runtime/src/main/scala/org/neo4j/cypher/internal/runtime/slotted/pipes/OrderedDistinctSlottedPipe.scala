/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.impl.factory.Sets
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

case class OrderedDistinctSlottedPipe(source: Pipe,
                                      slots: SlotConfiguration,
                                      groupingExpression: GroupingExpression)
                                     (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  groupingExpression.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[CypherRow],
                                      state: QueryState): Iterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private var seen = Sets.mutable.empty[AnyValue]()
      private var currentOrderedGroupingValue: AnyValue = _

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

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

  protected def internalCreateResults(input: Iterator[CypherRow],
                                      state: QueryState): Iterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private var currentOrderedGroupingValue: AnyValue = _

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

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
