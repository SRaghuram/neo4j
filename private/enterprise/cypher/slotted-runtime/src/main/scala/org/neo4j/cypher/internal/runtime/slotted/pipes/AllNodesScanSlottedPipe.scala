/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

case class AllNodesScanSlottedPipe(ident: String, slots: SlotConfiguration, argumentSize: Size)
                                  (val id: Id = Id.INVALID_ID) extends Pipe {

  private val offset = slots.getLongOffsetFor(ident)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    PrimitiveLongHelper.map(state.query.nodeOps.allPrimitive, { nodeId =>
      val context = SlottedExecutionContext(slots)
      state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
      context.setLongAt(offset, nodeId)
      context
    })
  }
}
