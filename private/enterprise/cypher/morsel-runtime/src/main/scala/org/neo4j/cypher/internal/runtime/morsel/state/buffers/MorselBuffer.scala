/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import java.util

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.{FilteringMorselExecutionContext, MorselExecutionContext}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateMaps, WorkCanceller}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.{AccumulatingBuffer, DataHolder, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentCountUpdater, ArgumentStateMap, MorselParallelizer, QueryCompletionTracker}
import org.neo4j.util.Preconditions

/**
  * Morsel buffer which adds reference counting of arguments to the regular buffer semantics.
  *
  * This buffer sits between two pipelines in the normal case.
  *
  * @param inner inner buffer to delegate real buffer work to
  */
class MorselBuffer(id: BufferId,
                   tracker: QueryCompletionTracker,
                   downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
                   workCancellers: IndexedSeq[ArgumentStateMapId],
                   override val argumentStateMaps: ArgumentStateMaps,
                   inner: Buffer[MorselExecutionContext]
                  ) extends ArgumentCountUpdater
                    with Sink[MorselExecutionContext]
                    with Source[MorselParallelizer]
                    with SinkByOrigin
                    with DataHolder {

  private val cancellerMaps = {
    val x = new Array[ArgumentStateMap[WorkCanceller]](workCancellers.size)
    var i = 0
    while (i < workCancellers.size) {
      x(i) = argumentStateMaps(workCancellers(i)).asInstanceOf[ArgumentStateMap[WorkCanceller]]
      i += 1
    }
    x
  }

  override def sinkFor[T <: AnyRef](fromPipeline: PipelineId): Sink[T] = this.asInstanceOf[Sink[T]]

  override def put(morsel: MorselExecutionContext): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- $morsel")
    }
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
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[putInDelegate] $this <- $morsel")
    }
    if (morsel.hasData) {
      morsel.resetToFirstRow()
      tracker.increment()
      inner.put(morsel)
    }
  }

  override def hasData: Boolean = inner.hasData

  override def take(): MorselParallelizer = {
    val morsel = inner.take()
    if (morsel == null) {
      null
    } else {
      if (DebugSupport.BUFFERS.enabled) {
        DebugSupport.BUFFERS.log(s"[take]  $this -> $morsel")
      }
      new Parallelizer(morsel)
    }
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
    if (workCancellers.nonEmpty) {
      val currentRow = morsel.getCurrentRow // Save current row

      Preconditions.checkArgument(morsel.isInstanceOf[FilteringMorselExecutionContext],
        s"Expected filtering morsel for filterCancelledArguments in buffer $id, but got ${morsel.getClass}")

      val filteringMorsel = morsel.asInstanceOf[FilteringMorselExecutionContext]
      filteringMorsel.resetToFirstRow()

      val reducerArguments = new Array[Long](downstreamArgumentReducers.size)
      util.Arrays.fill(reducerArguments, -1) // otherwise we do not decrement for argumentRowId 0

      while (filteringMorsel.isValidRow) {
        var i = 0
        var isCancelled = false
        var argumentRowId: Long = -1L
        var argumentSlotOffset: Int = -2

        while (i < workCancellers.size && !isCancelled) {
          val cancellerMap = cancellerMaps(i)
          argumentRowId = filteringMorsel.getArgumentAt(cancellerMap.argumentSlotOffset)
          if (cancellerMap.peek(argumentRowId).isCancelled) {
            argumentSlotOffset = cancellerMap.argumentSlotOffset
            isCancelled = true // break
          }
          i += 1
        }
        if (isCancelled) {

          // Cancel all rows up to the next argument
          do {
            var i = 0
            while (i < downstreamArgumentReducers.size) {
              val reducer = downstreamArgumentReducers(i)
              val arg = filteringMorsel.getArgumentAt(reducer.argumentSlotOffset)
              if (arg != reducerArguments(i)) {
                reducer.decrement(arg)
                reducerArguments(i) = arg
              }
              i += 1
            }
            filteringMorsel.cancelCurrentRow()
            filteringMorsel.moveToNextRow()
          } while (filteringMorsel.isValidRow && filteringMorsel.getArgumentAt(argumentSlotOffset) == argumentRowId)

        } else {
          filteringMorsel.moveToNextRow()
        }
      }

      filteringMorsel.moveToRawRow(currentRow) // Restore current row exactly (so may point at a cancelled row)
    }

    morsel.isEmpty
  }

  /**
    * Decrement reference counters attached to `morsel`.
    */
  def close(morsel: MorselExecutionContext): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[close] $this -X- $morsel")
    }
    decrementArgumentCounts(downstreamArgumentReducers, morsel)
    tracker.decrement()
  }

  override def toString: String = s"MorselBuffer($id, $inner)"

  /**
    * Implementation of [[MorselParallelizer]] that ensures correct reference counting.
    */
  class Parallelizer(original: MorselExecutionContext) extends MorselParallelizer {
    private var usedOriginal = false

    override def originalForClosing: MorselExecutionContext = original

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
