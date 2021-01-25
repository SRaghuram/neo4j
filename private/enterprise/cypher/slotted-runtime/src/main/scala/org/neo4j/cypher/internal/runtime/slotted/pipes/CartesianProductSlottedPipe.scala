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
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

case class CartesianProductSlottedPipe(lhs: Pipe, rhs: Pipe,
                                       lhsLongCount: Int,
                                       lhsRefCount: Int,
                                       slots: SlotConfiguration,
                                       argumentSize: SlotConfiguration.Size)
                                      (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    lhs.createResults(state).flatMap {
      lhsCtx =>
        rhs.createResults(state) map {
          rhsCtx =>
            val context = SlottedRow(slots)
            context.copyAllFrom(lhsCtx)
            context.copyFromOffset(rhsCtx,
              sourceLongOffset = argumentSize.nLongs,
              sourceRefOffset = argumentSize.nReferences,
              targetLongOffset = lhsLongCount,
              targetRefOffset = lhsRefCount)
            context
        }
    }
  }
}
