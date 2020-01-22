/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
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
 * @param argumentStatesOnRHSOfThisApply   these are not reducers, so argument states like the one from Limit, which are
 *                                         not directly related to a Buffer. They live on the RHS of the Apply
 * @param argumentSlotOffset               slot to which argument row ids are written.
 * @param argumentReducersOnRHSOfThisApply ids of downstream logical plans which keep argument state for this apply.
 *                                         These are on the RHS of the Apply this Buffer is for.
 * @param argumentReducersOnTopOfThisApply ids of reducers _after_ the Apply this Buffer is for.
 */
class MorselApplyBuffer(id: BufferId,
                        argumentStatesOnRHSOfThisApply: IndexedSeq[ArgumentStateMapId],
                        argumentReducersOnRHSOfThisApply: IndexedSeq[AccumulatingBuffer],
                        argumentReducersOnTopOfThisApply: IndexedSeq[AccumulatingBuffer],
                        override val argumentStateMaps: ArgumentStateMaps,
                        argumentSlotOffset: Int,
                        idAllocator: IdAllocator,
                        delegates: IndexedSeq[MorselBuffer]
                       ) extends ArgumentCountUpdater
                         with Sink[MorselExecutionContext] {

  def put(morsel: MorselExecutionContext): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- $morsel")
    }
    if (morsel.hasData) {
      var argumentRowId = idAllocator.allocateIdBatch(morsel.getValidRows)

      morsel.resetToFirstRow()
      while (morsel.isValidRow) {
        morsel.setArgumentAt(argumentSlotOffset, argumentRowId)

        // We can initiate the reducers/limits on the RHS inside of this loop, since they use the argumentRowIds we are generating here.
        // This does not hold for reducers on top of this apply.

        // Reducers on the RHS need to be initiated
        // It is important that the morsel is positioned at the row of the just added argumentRowId, so that the reducer
        // can read the argument row ids for _its_ downstream reducers from that row of the morsel.
        initiateArgumentReducersHere(argumentReducersOnRHSOfThisApply, argumentRowId, morsel)

        // Initiate argument states for limit
        initiateArgumentStatesHere(argumentStatesOnRHSOfThisApply, argumentRowId, morsel)

        argumentRowId += 1
        morsel.moveToNextRow()
      }

      // Reducers after the apply need to be incremented
      incrementArgumentCounts(argumentReducersOnTopOfThisApply, morsel)

      var i = 0
      while (i < delegates.size) {
        delegates(i).putInDelegate(morsel.shallowCopy())
        i += 1
      }
    }
  }

  override def canPut: Boolean = delegates.forall(_.canPut)

  override def toString: String = s"MorselApplyBuffer($id, argumentSlotOffset=$argumentSlotOffset)"
}
