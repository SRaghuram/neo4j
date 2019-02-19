/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Extension of [[MorselBuffer]], which generates and writes argument row ids into given `argumentSlotOffset`.
  *
  * @param argumentSlotOffset slot to which argument row ids are written.
  */
class MorselArgumentBuffer(argumentSlotOffset: Int,
                           tracker: Tracker,
                           countersForThisBuffer: Seq[Id],
                           countersForOtherBuffers: Seq[Id],
                           argumentStateMaps: ArgumentStateMaps,
                           inner: Buffer[MorselExecutionContext],
                           idAllocator: IdAllocator
                          ) extends MorselBuffer(tracker, countersForOtherBuffers, argumentStateMaps, inner) {

  override def produce(morsel: MorselExecutionContext): Unit = {
    var argumentRowId = idAllocator.allocateIdBatch(morsel.getValidRows)

    morsel.resetToFirstRow()
    while (morsel.isValidRow) {
      morsel.setLongAt(argumentSlotOffset, argumentRowId)
      argumentRowId += 1
      morsel.moveToNextRow()
    }

    for {
      reducePlanId <- countersForThisBuffer
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.initiate(argumentId)
    morsel.setCounters(countersForThisBuffer)

    super.produce(morsel)
  }
}
