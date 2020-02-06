/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

case class CrossApplySlottedPipe(lhs: Pipe, rhs: Pipe,
                                 lhsLongCount: Int,
                                 lhsRefCount: Int,
                                 slots: SlotConfiguration,
                                 rhsArgumentSize: SlotConfiguration.Size)
                                (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe {
  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] =
    for {
      lhsCtx <- lhs.createResults(state)
      rhsState = state.withInitialContext(lhsCtx)
      rhsCtx <- rhs.createResults(rhsState)
    } yield {
      val context = SlottedRow(slots)
      lhsCtx.copyTo(context)
      rhsCtx.copyTo(context, sourceLongOffset = rhsArgumentSize.nLongs, sourceRefOffset = rhsArgumentSize.nReferences, targetLongOffset = lhsLongCount, targetRefOffset = lhsRefCount)
      context
    }
}
