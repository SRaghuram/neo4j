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
  * Morsel buffer which adds reference counting of arguments to the regular buffer semantics.
  *
  * @param inner inner buffer to delegate real buffer work to
  */
class MorselBuffer(tracker: QueryCompletionTracker,
                   downstreamArgumentReducers: Seq[Id],
                   workCancellers: Seq[Id],
                   argumentStateMaps: ArgumentStateMaps,
                   inner: Buffer[MorselExecutionContext]
                  ) extends ArgumentCountUpdater(tracker, argumentStateMaps)
                       with Sink[MorselExecutionContext]
                       with Source[MorselParallelizer] {

  override def put(morsel: MorselExecutionContext): Unit = {
    if (morsel.hasData) {
      morsel.resetToFirstRow()
      incrementArgumentCounts(downstreamArgumentReducers, morsel)
      tracker.increment()
      inner.put(morsel)
    }
  }

  override def hasData: Boolean = inner.hasData

  override def take(): MorselParallelizer = {
    val morsel = inner.take()
    if (morsel == null)
      null
    else
      new Parallelizer(morsel)
  }

  def filterCancelledArguments(morsel: MorselExecutionContext): Boolean = {
    for (cancellerPlanId <- workCancellers) {
      val argumentStateMap = argumentStateMaps(cancellerPlanId).asInstanceOf[ArgumentStateMap[WorkCanceller]]
      val cancelledArguments =
        argumentStateMap.filterCancelledArguments(morsel,
                                                  canceller => canceller.isCancelled)

      decrementArgumentCounts(downstreamArgumentReducers, cancelledArguments)
    }
    if (morsel.isEmpty) {
      tracker.decrement()
      true
    } else {
      false
    }
  }

  /**
    * Decrement reference counters attached to `morsel`.
    */
  def close(morsel: MorselExecutionContext): Unit = {
    decrementArgumentCounts(downstreamArgumentReducers, morsel)
    tracker.decrement()
  }

  class Parallelizer(original: MorselExecutionContext) extends MorselParallelizer {
    private var usedOriginal = false
    override def nextCopy: MorselExecutionContext = {
      if (!usedOriginal) {
        usedOriginal = true
        original
      } else {
        val copy = original.shallowCopy()
        incrementArgumentCounts(downstreamArgumentReducers, copy)
        tracker.increment()
        copy
      }
    }
  }
}
