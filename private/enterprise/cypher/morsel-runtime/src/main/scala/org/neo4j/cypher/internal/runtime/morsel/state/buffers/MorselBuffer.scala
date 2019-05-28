/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, PipelineId}
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateMaps, WorkCanceller}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.{AccumulatingBuffer, DataHolder, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentCountUpdater, ArgumentStateMap, MorselParallelizer, QueryCompletionTracker}

/**
  * Morsel buffer which adds reference counting of arguments to the regular buffer semantics.
  *
  * This buffer sits between two pipelines in the normal case.
  *
  * @param inner inner buffer to delegate real buffer work to
  */
class MorselBuffer(tracker: QueryCompletionTracker,
                   downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
                   workCancellers: IndexedSeq[ArgumentStateMapId],
                   argumentStateMaps: ArgumentStateMaps,
                   inner: Buffer[MorselExecutionContext]
                  ) extends ArgumentCountUpdater
                    with Sink[MorselExecutionContext]
                    with Source[MorselParallelizer]
                    with SinkByOrigin
                    with DataHolder {

  override def sinkFor[T <: AnyRef](fromPipeline: PipelineId): Sink[T] = this.asInstanceOf[Sink[T]]

  override def put(morsel: MorselExecutionContext): Unit = {
    if (morsel.hasData) {
      morsel.resetToFirstRow()
      incrementArgumentCounts(downstreamArgumentReducers, morsel)
      tracker.increment()
      inner.put(morsel)
    }
  }

  override def canPut: Boolean = inner.canPut

  /**
    * This is essentially the same as [[put]], except that no argument counts are incremented.
    * The reason is that if this is one of the delegates of a [[MorselApplyBuffer]], that
    * buffer took care of incrementing the right ones already.
    */
  def putInDelegate(morsel: MorselExecutionContext): Unit = {
    if (morsel.hasData) {
      morsel.resetToFirstRow()
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

  override def clearAll(): Unit = {
    var morsel = inner.take()
    while (morsel != null) {
      close(morsel)
      morsel = inner.take()
    }
  }

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    *
    * @param morsel the input morsel
    * @return `true` iff the morsel is cancelled
    */
  def filterCancelledArguments(morsel: MorselExecutionContext): Boolean = {
    for (workCanceller <- workCancellers) {
      val argumentStateMap = argumentStateMaps(workCanceller).asInstanceOf[ArgumentStateMap[WorkCanceller]]
      val cancelledArguments =
        argumentStateMap.filterCancelledArguments(morsel,
                                                  canceller => canceller.isCancelled)

      decrementArgumentCounts(downstreamArgumentReducers, cancelledArguments)
    }
    morsel.isEmpty
  }

  /**
    * Decrement reference counters attached to `morsel`.
    */
  def close(morsel: MorselExecutionContext): Unit = {
    decrementArgumentCounts(downstreamArgumentReducers, morsel)
    tracker.decrement()
  }

  override def toString: String = s"MorselBuffer($inner)"

  /**
    * Implementation of [[MorselParallelizer]] that ensures correct reference counting.
    */
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
