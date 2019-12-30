/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class CrossApplySlottedPipe(lhs: Pipe, rhs: Pipe,
                                 lhsLongCount: Int,
                                 lhsRefCount: Int,
                                 slots: SlotConfiguration,
                                 rhsArgumentSize: SlotConfiguration.Size)
                                (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe {
  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    for {
      lhsCtx <- lhs.createResults(state)
      rhsState = state.withInitialContext(lhsCtx)
      rhsCtx <- rhs.createResults(rhsState)
    } yield {
      val context = SlottedExecutionContext(slots)
      lhsCtx.copyTo(context)
      rhsCtx.copyTo(context,
        sourceLongOffset = rhsArgumentSize.nLongs, sourceRefOffset = rhsArgumentSize.nReferences, // Skip over arguments since they should be identical to lhsCtx
        targetLongOffset = lhsLongCount, targetRefOffset = lhsRefCount)
      context
    }
}
