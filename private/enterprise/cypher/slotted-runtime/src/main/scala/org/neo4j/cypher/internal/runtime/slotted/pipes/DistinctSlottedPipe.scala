/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.values.AnyValue

case class DistinctSlottedPipe(source: Pipe,
                               slots: SlotConfiguration,
                               groupingExpression: GroupingExpression)
                              (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  protected def internalCreateResults(input: Iterator[CypherRow],
                                      state: QueryState): Iterator[CypherRow] = {
    new PrefetchingIterator[CypherRow] {
      private var seen = DistinctSet.createDistinctSet[AnyValue](state.memoryTracker.memoryTrackerForOperator(id.x))

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) {
          val next: CypherRow = input.next()

          val key = groupingExpression.computeGroupingKey(next, state)
          if (seen.add(key)) {
            // Found unseen key! Set it as the next element to yield, and exit
            groupingExpression.project(next, key)
            return Some(next)
          }
        }
        seen.close()
        seen = null
        None
      }
    }
  }
}
