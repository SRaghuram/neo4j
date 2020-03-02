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
  extends OptionalMorselBuffer(id, tracker, downstreamArgumentReducers, argumentStateMaps, argumentStateMapId) {

  override def take(): MorselData = {
    // To keep input order (i.e., place the null rows at the right position), we give the data out in ascending argument row id order.

    // Optimization: Could proceed before an argument 'is completed', as soon as 'did receive data' is true.
    //               At that point it is already known that the argument should be filtered out.
    //               In practice, Anti is always on top of a Limit 1, so there is no point it optimizing for other cases.
    val argumentState: OptionalArgumentStateBuffer = argumentStateMap.takeNextIfCompleted()
    val data: MorselData =
      if (argumentState != null) {
        // NOTE: even though these morsels will not be used, they need to be returned so on close the counts are decremented correctly
        val allMorsels = argumentState.takeAll()
        val morsels = if (null != allMorsels) allMorsels else IndexedSeq.empty
        if (!argumentState.didReceiveData) {
          MorselData(morsels, EndOfEmptyStream(argumentState.argumentRow), argumentState.argumentRowIdsForReducers)
        } else {
          // We need to return this message to signal that the end of the stream was reached (even if some other Thread got the morsels
          // before us), to close and decrement correctly.
          MorselData(morsels, EndOfNonEmptyStream, argumentState.argumentRowIdsForReducers)
        }
      } else {
        null.asInstanceOf[MorselData]
      }
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[take]  $this -> $data")
    }
    data
  }

  override def hasData: Boolean = {
    // Optimization: Could proceed before an argument 'is completed', as soon as 'did receive data' is true.
    //               At that point it is already known that the argument should be filtered out.
    //               In practice, Anti is always on top of a Limit 1, so there is no point it optimizing for other cases.
    argumentStateMap.nextArgumentStateIsCompletedOr(_ => false)
  }

  override def toString: String =
    s"AntiMorselBuffer(planId: $argumentStateMapId)$argumentStateMap"
}
