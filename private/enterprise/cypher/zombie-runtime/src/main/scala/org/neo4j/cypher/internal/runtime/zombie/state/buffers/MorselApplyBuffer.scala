/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, PipelineId}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffers.{AccumulatingBuffer, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.zombie.state.{ArgumentCountUpdater, IdAllocator}

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
class MorselApplyBuffer(argumentStatesOnRHSOfThisApply: IndexedSeq[ArgumentStateMapId],
                        argumentReducersOnRHSOfThisApply: IndexedSeq[AccumulatingBuffer],
                        argumentReducersOnTopOfThisApply: IndexedSeq[AccumulatingBuffer],
                        argumentStateMaps: ArgumentStateMaps,
                        argumentSlotOffset: Int,
                        idAllocator: IdAllocator,
                        delegates: IndexedSeq[MorselBuffer]
                       ) extends ArgumentCountUpdater
                         with SinkByOrigin
                         with Sink[MorselExecutionContext] {

  override def sinkFor(fromPipeline: PipelineId): Sink[MorselExecutionContext] = this

  def put(morsel: MorselExecutionContext): Unit = {
    if (morsel.hasData) {
      var argumentRowId = idAllocator.allocateIdBatch(morsel.getValidRows)

      morsel.resetToFirstRow()
      while (morsel.isValidRow) {
        morsel.setLongAt(argumentSlotOffset, argumentRowId)
        argumentRowId += 1
        morsel.moveToNextRow()
      }

      // initiate argument states for limit
      for {
        argumentStateMapId <- argumentStatesOnRHSOfThisApply
        asm = argumentStateMaps(argumentStateMapId)
        argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
      } asm.initiate(argumentId)

      // Reducers on the RHS need to be initiated
      initiateArgumentReducers(argumentReducersOnRHSOfThisApply, morsel)
      // And reducers after the apply need to be incremented
      incrementArgumentCounts(argumentReducersOnTopOfThisApply, morsel)
      var i = 0
      while (i < delegates.size) {
        delegates(i).putByApply(morsel.shallowCopy())
        i += 1
      }
    }
  }
}
