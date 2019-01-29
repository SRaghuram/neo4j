/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.impl.factory.Sets
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
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
        while (input.nonEmpty) {
          val next: ExecutionContext = input.next()

          val array = buildKey(next)
          if (seen.add(array)) {
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

  private def buildKey(next: ExecutionContext): LongArray = {
    val keys = new Array[Long](primitiveSlots.length)
    var i = 0
    while (i < primitiveSlots.length) {
      keys(i) = next.getLongAt(primitiveSlots(i))
      i += 1
    }
    Values.longArray(keys)
  }
}
