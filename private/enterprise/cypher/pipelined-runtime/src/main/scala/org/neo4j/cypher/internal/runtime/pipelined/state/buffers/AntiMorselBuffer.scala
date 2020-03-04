/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer

/**
 * Extension of OptionalMorselBuffer.
 * Holds an ASM in order to track argument rows that do not result in any output rows, i.e. gets filtered out.
 * This is used in front of a pipeline with an AntiOperator.
 * This buffer sits between two pipelines.
 */
class AntiMorselBuffer(id: BufferId,
                       tracker: QueryCompletionTracker,
                       downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
                       argumentStateMaps: ArgumentStateMaps,
                       argumentStateMapId: ArgumentStateMapId
                      )
  extends BaseArgExistsMorselBuffer[MorselData](id, tracker, downstreamArgumentReducers, argumentStateMaps, argumentStateMapId) {

  override def take(): MorselData = {
    // To keep input order (i.e., place the null rows at the right position), we give the data out in ascending argument row id order.
    // Potential Optimization: return multiple rows
    val data = getNextEmptyArgumentState
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[take]  $this -> $data")
    }
    data
  }

  private def getNextEmptyArgumentState: MorselData = {
    var argumentState = argumentStateMap.takeNextIfCompleted()
    while (argumentState != null) {
      if (argumentState.didReceiveData) {
        val allMorsels = argumentState.takeAll()
        val morsels = if (null != allMorsels) allMorsels else IndexedSeq.empty
        // no need to return end of non-empty stream, but we need to close so counts are updated
        closeOne(EndOfNonEmptyStream, morsels.size, argumentState.argumentRowIdsForReducers)
        argumentState = argumentStateMap.takeNextIfCompleted()
      } else {
        return MorselData(IndexedSeq.empty, EndOfEmptyStream(argumentState.argumentRow), argumentState.argumentRowIdsForReducers)
      }
    }
    null.asInstanceOf[MorselData]
  }

  override def hasData: Boolean = {
    argumentStateMap.nextArgumentStateIsCompletedOr(_ => false)
  }

  override def close(data: MorselData): Unit = {
    closeOne(data.argumentStream, numberOfDecrements = 0, data.argumentRowIdsForReducers)
  }

  override def toString: String =
    s"AntiMorselBuffer(planId: $argumentStateMapId)$argumentStateMap"
}
