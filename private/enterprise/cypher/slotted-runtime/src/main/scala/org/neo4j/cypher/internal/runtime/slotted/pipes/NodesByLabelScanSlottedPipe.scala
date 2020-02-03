/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.attribution.Id

case class NodesByLabelScanSlottedPipe(ident: String,
                                       label: LazyLabel,
                                       slots: SlotConfiguration,
                                       argumentSize: SlotConfiguration.Size)
                                      (val id: Id = Id.INVALID_ID) extends Pipe {

  private val offset = slots.getLongOffsetFor(ident)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {

    val labelId = label.getId(state.query)
    if (labelId == LazyLabel.UNKNOWN) Iterator.empty
    else {
      PrimitiveLongHelper.map(state.query.getNodesByLabelPrimitive(labelId), { nodeId =>
        val context = SlottedExecutionContext(slots)
        state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
        context.setLongAt(offset, nodeId)
        context
      })
    }
  }
}
