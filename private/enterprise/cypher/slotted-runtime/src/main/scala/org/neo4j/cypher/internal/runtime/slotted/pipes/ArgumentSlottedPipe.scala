/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

case class ArgumentSlottedPipe(slots: SlotConfiguration,
                               argumentSize:Size)
                              (val id: Id = Id.INVALID_ID)
  extends Pipe {

  def internalCreateResults(state: QueryState): Iterator[SlottedExecutionContext] = {
    val context = SlottedExecutionContext(slots)
    state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
    Iterator(context)
  }
}
