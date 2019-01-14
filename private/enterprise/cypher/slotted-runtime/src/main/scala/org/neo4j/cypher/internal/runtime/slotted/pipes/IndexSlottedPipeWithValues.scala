/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexIteratorBase, Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.internal.kernel.api.NodeValueIndexCursor

/**
  * Provides helper methods for slotted index pipes that get nodes together with actual property values.
  */
trait IndexSlottedPipeWithValues extends Pipe {

  // Offset of the long slot of node variable
  val offset: Int
  // the indices of the index properties where we will get values
  val indexPropertyIndices: Array[Int]
  // the offsets of the cached node property slots where we will set values
  val indexPropertySlotOffsets: Array[Int]
  // Number of longs and refs
  val argumentSize: SlotConfiguration.Size

  class SlottedIndexIterator(state: QueryState,
                             slots: SlotConfiguration,
                             cursor: NodeValueIndexCursor
                            ) extends IndexIteratorBase[ExecutionContext](cursor) {

    override protected def fetchNext(): ExecutionContext = {
      if (cursor.next()) {
        val slottedContext = SlottedExecutionContext(slots)
        state.copyArgumentStateTo(slottedContext, argumentSize.nLongs, argumentSize.nReferences)
        slottedContext.setLongAt(offset, cursor.nodeReference())
        var i = 0
        while (i < indexPropertyIndices.length) {
          val value = cursor.propertyValue(indexPropertyIndices(i))
          slottedContext.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
          i += 1
        }
        slottedContext
      } else null
    }
  }
}
