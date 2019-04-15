/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, PipelineId}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentStateMaps, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffers.{AccumulatingBuffer, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.zombie.state.{ArgumentCountUpdater, ArgumentStateMap, QueryCompletionTracker}

/**
  * Morsel buffer that groups incoming rows by argumentRowId by delegating to an [[ArgumentStateMap]].
  *
  * This buffer sits before a pipeline that starts with a reducer. It accumulates the state into the
  * ArgumentStateMap.
  *
  * @param argumentStateMapId id of the argument state map to reduce data into.
  */
class MorselArgumentStateBuffer[ACC <: MorselAccumulator](tracker: QueryCompletionTracker,
                                                          downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
                                                          argumentStateMaps: ArgumentStateMaps,
                                                          val argumentStateMapId: ArgumentStateMapId
                                                         ) extends ArgumentCountUpdater
                                                           with AccumulatingBuffer
                                                           with Sink[MorselExecutionContext]
                                                           with Source[ACC]
                                                           with SinkByOrigin {

  private val argumentStateMap: ArgumentStateMap[ACC] = argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[ACC]]

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  override def sinkFor(fromPipeline: PipelineId): Sink[MorselExecutionContext] = this

  override def put(morsel: MorselExecutionContext): Unit = {
    if (morsel.hasData) {
      morsel.resetToFirstRow()
      argumentStateMap.update(morsel, (acc, morselView) => {
        acc.update(morselView)
      })
    }
  }

  override def hasData: Boolean = argumentStateMap.hasCompleted

  override def take(): ACC = {
    argumentStateMap.takeOneCompleted()
  }

  override def initiate(argumentRowId: Long): Unit = {
    argumentStateMap.initiate(argumentRowId)
    // TODO Sort-Apply-Sort-Bug: the downstream might have different argument IDs to care about
    incrementArgumentCounts(downstreamArgumentReducers, IndexedSeq(argumentRowId))
    tracker.increment()
  }

  override def increment(argumentRowId: Long): Unit = {
    argumentStateMap.increment(argumentRowId)
  }

  override def decrement(argumentRowId: Long): Unit = {
    argumentStateMap.decrement(argumentRowId)
  }

  /**
    * Decrement reference counters attached to `accumulator`.
    */
  def close(accumulator: ACC): Unit = {
    // TODO Sort-Apply-Sort-Bug: the downstream might have different argument IDs to care about
    decrementArgumentCounts(downstreamArgumentReducers, IndexedSeq(accumulator.argumentRowId))
    tracker.decrement()
  }

  /**
    * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
    *
    * @param accumulator the accumulator
    * @return `true` iff the accumulator is cancelled
    */
  def filterCancelledArguments(accumulator: ACC): Boolean = {
    // TODO
    false
  }

  override def toString: String =
    s"MorselArgumentStateBuffer(planId: $argumentStateMapId)$argumentStateMap"
}
