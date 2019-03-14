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
                        argumentStatesForThisApply: Seq[Id],
                        argumentReducersForOtherApplies: Seq[Id],
                        argumentStateMaps: ArgumentStateMaps,
                        inner: Buffer[MorselExecutionContext],
                        argumentSlotOffset: Int,
                        idAllocator: IdAllocator
                          ) extends MorselBuffer(tracker,
                                                 argumentReducersForOtherApplies,
                                                 argumentStateMaps,
                                                 inner) {

  override def put(morsel: MorselExecutionContext): Unit = {
    var argumentRowId = idAllocator.allocateIdBatch(morsel.getValidRows)

    morsel.resetToFirstRow()
    while (morsel.isValidRow) {
      morsel.setLongAt(argumentSlotOffset, argumentRowId)
      argumentRowId += 1
      morsel.moveToNextRow()
    }

    initiateArgumentCounts(argumentStatesForThisApply, morsel)

    super.put(morsel)
  }
}
