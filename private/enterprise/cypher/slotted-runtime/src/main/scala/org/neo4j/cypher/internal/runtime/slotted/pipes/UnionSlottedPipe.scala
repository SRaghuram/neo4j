/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe.RowMapping
import org.neo4j.cypher.internal.util.attribution.Id

case class UnionSlottedPipe(lhs: Pipe,
                            rhs: Pipe,
                            slots: SlotConfiguration,
                            lhsMapping: RowMapping,
                            rhsMapping: RowMapping)
                           (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)

    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = left.hasNext || right.hasNext

      override def next(): ExecutionContext = {
        val outgoing = SlottedExecutionContext(slots)
        if (left.hasNext) {
          val incoming =  left.next()
          lhsMapping.mapRows(incoming, outgoing, state)
        } else {
          val incoming = right.next()
          rhsMapping.mapRows(incoming, outgoing, state)
        }
        outgoing
      }
    }
  }
}

object UnionSlottedPipe {
  trait RowMapping {
    def mapRows(incoming: ExecutionContext, outgoing: ExecutionContext, state: QueryState): Unit
  }
}
