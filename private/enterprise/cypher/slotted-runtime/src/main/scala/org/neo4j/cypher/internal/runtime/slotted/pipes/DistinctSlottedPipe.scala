/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.impl.factory.Sets
import org.neo4j.cypher.internal.physical_planning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.AnyValue

case class DistinctSlottedPipe(source: Pipe,
                               slots: SlotConfiguration,
                               distinctProjection: GroupingExpression)
                              (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  distinctProjection.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    new PrefetchingIterator[ExecutionContext] {
      private val seen = Sets.mutable.empty[AnyValue]()

      override def produceNext(): Option[ExecutionContext] = {
        while (input.hasNext) {
          val next: ExecutionContext = input.next()

          val key = distinctProjection.computeGroupingKey(next, state)
          if (seen.add(key)) {
            // Found unseen key! Set it as the next element to yield, and exit
            val outgoing = SlottedExecutionContext(slots)
            outgoing.copyCachedFrom(next)
            outgoing.setLinenumber(next.getLinenumber)
            distinctProjection.project(outgoing, key)
            return Some(outgoing)
          }
        }
        None
      }
    }
  }
}
