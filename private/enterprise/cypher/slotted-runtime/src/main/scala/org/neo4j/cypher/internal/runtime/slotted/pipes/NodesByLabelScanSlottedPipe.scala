/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyLabel, Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class NodesByLabelScanSlottedPipe(ident: String,
                                       label: LazyLabel,
                                       slots: SlotConfiguration,
                                       argumentSize: SlotConfiguration.Size)
                                      (val id: Id = Id.INVALID_ID) extends Pipe {

  private val offset = slots.getLongOffsetFor(ident)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    label.getOptId(state.query) match {
      case Some(labelId) =>
        PrimitiveLongHelper.map(state.query.getNodesByLabelPrimitive(labelId.id), { nodeId =>
          val context = SlottedExecutionContext(slots)
          state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
          context.setLongAt(offset, nodeId)
          context
        })
      case None =>
        Iterator.empty
    }
  }
}
