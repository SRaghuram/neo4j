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
  * Extension of [[MorselBuffer]] which connect the LHS and RHS of an Apply. To allow tracking the data from a
  * particular argument rows, this buffer generates and writes argument row ids into a given `argumentSlotOffset`.
  *
  * @param argumentSlotOffset         slot to which argument row ids are written.
  * @param argumentStatesForThisApply ids of downstream logical plans which keep argument state for this apply
  */
class MorselApplyBuffer(tracker: QueryCompletionTracker,
                        argumentStatesToInitiate: Seq[Id],
                        argumentReducersForThis: Seq[MorselArgumentStateBuffer[_]],
                        argumentReducersForOtherApplies: Seq[Id],
                        workCancellers: Seq[Id],
                        argumentStateMaps: ArgumentStateMaps,
                        inner: Buffer[MorselExecutionContext],
                        argumentSlotOffset: Int,
                        idAllocator: IdAllocator
                          ) extends MorselBuffer(tracker,
                                                 argumentReducersForOtherApplies,
                                                 workCancellers,
                                                 argumentStateMaps,
                                                 inner) {

  override def put(morsel: MorselExecutionContext): Unit = {
    if (morsel.hasData) {
      var argumentRowId = idAllocator.allocateIdBatch(morsel.getValidRows)

      morsel.resetToFirstRow()
      while (morsel.isValidRow) {
        morsel.setLongAt(argumentSlotOffset, argumentRowId)
        argumentRowId += 1
        morsel.moveToNextRow()
      }

      initiateArgumentStates(argumentStatesToInitiate, morsel)
      for (buffer <- argumentReducersForThis)
        buffer.initiate(morsel)

      super.put(morsel)
    }
  }

  override def close(morsel: MorselExecutionContext): Unit = {
    for (buffer <- argumentReducersForThis)
      buffer.decrement(morsel)
    super.close(morsel)
  }
}
