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
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer

/**
 * Extension of Morsel buffer that also holds an argument state map in order to track
 * argument rows that do not result in any output rows, i.e. gets filtered out.
 *
 * This is used in front of a pipeline with an OptionalOperator.
 *
 * This buffer sits between two pipelines.
 */
class OptionalMorselBuffer(id: BufferId,
                           tracker: QueryCompletionTracker,
                           downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
                           argumentStateMaps: ArgumentStateMaps,
                           argumentStateMapId: ArgumentStateMapId
                          )
  extends BaseArgExistsMorselBuffer[MorselData](id, tracker, downstreamArgumentReducers, argumentStateMaps, argumentStateMapId) {

  override def take(): MorselData = {
    // To achieve streaming behavior, we peek at the data, even if it is not completed yet.
    // To keep input order (i.e., place the null rows at the right position), we give the
    // data out in ascending argument row id order (only in pipelined).
    val argumentState = argumentStateMap.takeOneIfCompletedOrElsePeek()
    val data: MorselData =
      if (argumentState != null) {
        argumentState match {
          case ArgumentStateWithCompleted(completedArgument, true) =>
            if (!completedArgument.didReceiveData) {
              MorselData(IndexedSeq.empty, EndOfEmptyStream(completedArgument.argumentRow), completedArgument.argumentRowIdsForReducers)
            } else {
              val morsels = completedArgument.takeAll()
              if (morsels != null) {
                MorselData(morsels, EndOfNonEmptyStream, completedArgument.argumentRowIdsForReducers)
              } else {
                // We need to return this message to signal that the end of the stream was reached (even if some other Thread got the morsels
                // before us), to close and decrement correctly.
                MorselData(IndexedSeq.empty, EndOfNonEmptyStream, completedArgument.argumentRowIdsForReducers)
              }
            }

          case ArgumentStateWithCompleted(incompleteArgument, false) =>
            val morsels = incompleteArgument.takeAll()
            if (morsels != null) {
              MorselData(morsels, NotTheEnd, incompleteArgument.argumentRowIdsForReducers)
            } else {
              // In this case we can simply not return anything, there will arrive more data for this argument row id.
              null.asInstanceOf[MorselData]
            }
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
    argumentStateMap.someArgumentStateIsCompletedOr(state => state.hasData)
  }

  override def close(data: MorselData): Unit = {
    closeOne(data.argumentStream, data.morsels.size, data.argumentRowIdsForReducers)
  }

  override def toString: String =
    s"OptionalMorselBuffer(planId: $argumentStateMapId)$argumentStateMap"
}
