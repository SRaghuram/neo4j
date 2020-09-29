/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndPayload
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder

trait JoinBuffer[ACC_DATA <: AnyRef,
  LHS_ACC <: MorselAccumulator[ACC_DATA],
  PAYLOAD <: AnyRef
] extends ArgumentCountUpdater
  with Source[AccumulatorAndPayload[ACC_DATA, LHS_ACC, PAYLOAD]]
  with DataHolder {

  def filterCancelledArguments(accumulator: MorselAccumulator[_], rhsMorsel: Morsel): Boolean

  def close(accumulator: MorselAccumulator[_], loggable: PAYLOAD)
}

object JoinBuffer {
  def removeArgumentStatesIfRhsBufferIsEmpty(lhsArgumentStateMap: ArgumentStateMap[_ <: ArgumentState],
                                             rhsArgumentStateMap: ArgumentStateMap[_ <: ArgumentStateBuffer],
                                             rhsBuffer: ArgumentStateBuffer,
                                             argumentRowId: Long): Unit = {
    // If all data is processed and we are the first thread to remove the RHS accumulator (there can be a race here)
    if (rhsBuffer != null && !rhsBuffer.hasData && rhsArgumentStateMap.remove(argumentRowId) != null) {
      // Clean up the LHS as well
      val lhsAccumulator = lhsArgumentStateMap.remove(argumentRowId)
      if (lhsAccumulator != null) {
        lhsAccumulator.close()
      }
    }
  }
}