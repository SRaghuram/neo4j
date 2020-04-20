/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.Initialization
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.IdAllocator
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer

/**
 * This buffer connects the LHS input of an Apply to all its RHS leafs.
 * Multiplexer to a set of [[MorselBuffer]].
 * To allow tracking the data from a
 * particular argument rows, this buffer generates and writes argument row ids into a given `argumentSlotOffset`.
 *
 * @param workCancellersOnRHSOfThisApply   limit or skip on the RHS of the Apply.
 * @param argumentStatesOnRHSOfThisApply   these are not reducers, so argument states like the one from Distinct, which are
 *                                         not directly related to a Buffer. They live on the RHS of the Apply
 * @param argumentSlotOffset               slot to which argument row ids are written.
 * @param argumentReducersOnRHSOfThisApply ids of downstream logical plans which keep argument state for this apply.
 *                                         These are on the RHS of the Apply this Buffer is for.
 * @param argumentReducersOnTopOfThisApply ids of reducers _after_ the Apply this Buffer is for.
 */
class MorselApplyBuffer(id: BufferId,
                        workCancellersOnRHSOfThisApply: ReadOnlyArray[Initialization[ArgumentStateMapId]],
                        argumentStatesOnRHSOfThisApply: ReadOnlyArray[ArgumentStateMapId],
                        argumentReducersOnRHSOfThisApply: ReadOnlyArray[Initialization[AccumulatingBuffer]],
                        argumentReducersOnTopOfThisApply: ReadOnlyArray[AccumulatingBuffer],
                        override val argumentStateMaps: ArgumentStateMaps,
                        argumentSlotOffset: Int,
                        idAllocator: IdAllocator,
                        delegates: ReadOnlyArray[MorselBuffer]
                       ) extends ArgumentCountUpdater
                         with Sink[Morsel] {

  def put(morsel: Morsel): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- $morsel")
    }
    if (morsel.hasData) {
      var argumentRowId = idAllocator.allocateIdBatch(morsel.numberOfRows)

      val cursor = morsel.writeCursor()
      val readCursor = morsel.readCursor()
      while (cursor.next()) {
        ArgumentSlots.setArgumentAt(cursor, argumentSlotOffset, argumentRowId)
        readCursor.setRow(cursor.row)

        // We can initiate the reducers/limits on the RHS inside of this loop, since they use the argumentRowIds we are generating here.
        // This does not hold for reducers on top of this apply.

        // Reducers on the RHS need to be initiated
        // It is important that the readCursor is positioned at the row of the just added argumentRowId, so that the reducer
        // can read the argument row ids for _its_ downstream reducers from that row of the morsel.
        initiateArgumentReducersHere(argumentReducersOnRHSOfThisApply, argumentRowId, readCursor)

        // Initiate canceller argument states for limit/skip
        initiateWorkCancellerArgumentStatesHere(workCancellersOnRHSOfThisApply, argumentRowId, readCursor)

        // Initiate argument states for distinct
        initiateArgumentStatesHere(argumentStatesOnRHSOfThisApply, argumentRowId, readCursor)

        argumentRowId += 1
      }

      // Reducers after the apply need to be incremented
      incrementArgumentCounts(argumentReducersOnTopOfThisApply, morsel)

      var i = 0
      while (i < delegates.length) {
        delegates(i).putInDelegate(morsel.shallowCopy())
        i += 1
      }
    }
  }

  override def canPut: Boolean = {
    var i = 0
    while (i < delegates.length) {
      if (!delegates(i).canPut)
        return false
      i += 1
    }
    true
  }

  override def toString: String = s"MorselApplyBuffer($id, argumentSlotOffset=$argumentSlotOffset)"
}
