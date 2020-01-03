/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class CartesianProductSlottedPipe(lhs: Pipe, rhs: Pipe,
                                       lhsLongCount: Int,
                                       lhsRefCount: Int,
                                       slots: SlotConfiguration,
                                       argumentSize: SlotConfiguration.Size)
                                      (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    lhs.createResults(state) flatMap {
      lhsCtx =>
        rhs.createResults(state) map {
          rhsCtx =>
            val context = SlottedExecutionContext(slots)
            lhsCtx.copyTo(context)
            rhsCtx.copyTo(context,
              sourceLongOffset = argumentSize.nLongs, sourceRefOffset = argumentSize.nReferences, // Skip over arguments since they should be identical to lhsCtx
              targetLongOffset = lhsLongCount, targetRefOffset = lhsRefCount)
            context
        }
    }
  }
}
