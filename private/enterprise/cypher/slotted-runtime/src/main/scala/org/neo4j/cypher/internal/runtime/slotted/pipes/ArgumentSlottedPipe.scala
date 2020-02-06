/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

case class ArgumentSlottedPipe(slots: SlotConfiguration,
                               argumentSize:Size)
                              (val id: Id = Id.INVALID_ID)
  extends Pipe {

  def internalCreateResults(state: QueryState): Iterator[SlottedRow] = {
    val context = SlottedRow(slots)
    state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
    Iterator(context)
  }
}
