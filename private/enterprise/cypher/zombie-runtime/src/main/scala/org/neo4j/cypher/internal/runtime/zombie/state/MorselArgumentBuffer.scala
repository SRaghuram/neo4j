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
                           counters: Seq[Id],
                           argumentStateMaps: ArgumentStateMaps,
                           inner: Buffer[MorselExecutionContext]
                          ) extends MorselBuffer(tracker, counters, argumentStateMaps, inner) {

  private var argumentRowCount = 0L

  override def produce(morsel: MorselExecutionContext): Unit = {
    morsel.resetToFirstRow()
    while (morsel.isValidRow) {
      morsel.setLongAt(argumentSlotOffset, argumentRowCount)
      argumentRowCount += 1
      morsel.moveToNextRow()
    }
    morsel.resetToFirstRow()
    super.produce(morsel)
  }
}
