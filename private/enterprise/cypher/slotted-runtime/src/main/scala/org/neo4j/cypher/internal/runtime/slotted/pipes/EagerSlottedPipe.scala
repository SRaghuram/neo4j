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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.EagerBuffer

import scala.collection.JavaConverters.asScalaIteratorConverter

case class EagerSlottedPipe(source: Pipe, slots: SlotConfiguration)(val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    val buffer = EagerBuffer.createEagerBuffer[CypherRow](state.memoryTracker.memoryTrackerForOperator(id.x),
                                                          1024,
                                                          8192,
                                                          EagerBuffer.GROW_NEW_CHUNKS_BY_100_PCT
                                                          )
    state.query.resources.trace(buffer)
    while (input.hasNext) {
      buffer.add(input.next)
    }
    ClosingIterator(buffer.autoClosingIterator().asScala).map { bufferedRow =>
      // this is necessary because Eager is the beginning of a new pipeline
      // We do this on the output side, and buffer the input rows they will use less memory
      val outputRow = SlottedRow(slots)
      outputRow.copyAllFrom(bufferedRow)
      outputRow
    }.closing(buffer)
  }
}
