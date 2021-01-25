/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.RowMapping
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

case class UnionSlottedPipe(lhs: Pipe,
                            rhs: Pipe,
                            slots: SlotConfiguration,
                            lhsMapping: RowMapping,
                            rhsMapping: RowMapping)
                           (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)

    new ClosingIterator[CypherRow] {
      override def innerHasNext: Boolean = left.hasNext || right.hasNext

      override def next(): CypherRow = {
        val outgoing = SlottedRow(slots)
        if (left.hasNext) {
          val incoming =  left.next()
          lhsMapping.mapRows(incoming, outgoing, state)
        } else {
          val incoming = right.next()
          rhsMapping.mapRows(incoming, outgoing, state)
        }
        outgoing
      }

      override def closeMore(): Unit = {
        left.close()
        right.close()
      }

    }
  }
}
