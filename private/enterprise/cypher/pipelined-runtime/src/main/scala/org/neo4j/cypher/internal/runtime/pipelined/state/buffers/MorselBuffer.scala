/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import java.util

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.{FilteringPipelinedExecutionContext, MorselExecutionContext}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentStateMaps, WorkCanceller}
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.{AccumulatingBuffer, DataHolder, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselBuffer._
import org.neo4j.cypher.internal.runtime.pipelined.state.{ArgumentCountUpdater, ArgumentStateMap, MorselParallelizer, QueryCompletionTracker}
import org.neo4j.util.Preconditions

object MorselBuffer {
  private val INVALID_ARG_ROW_ID = -1L
  private val INVALID_ARG_SLOT_OFFSET = -2
}

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

  private val cancellerASMs = {
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

      Preconditions.checkArgument(morsel.isInstanceOf[FilteringPipelinedExecutionContext],
                                  s"Expected filtering morsel for filterCancelledArguments in buffer $id, but got ${morsel.getClass}")

      val filteringMorsel = morsel.asInstanceOf[FilteringPipelinedExecutionContext]
      filteringMorsel.resetToFirstRow()

      val rowsToCancel = determineCancelledRows(filteringMorsel)

      // Second loop: decrement for reducers
      if(!rowsToCancel.isEmpty) {
        decrementReducers(filteringMorsel, rowsToCancel)
        // Actually cancel all rows
        filteringMorsel.cancelRows(rowsToCancel)
      }
      filteringMorsel.moveToRawRow(currentRow) // Restore current row exactly (so may point at a cancelled row)
    }

    morsel.isEmpty
  }

  private def decrementReducers(filteringMorsel: FilteringPipelinedExecutionContext, rowsToCancel: util.BitSet): Unit = {
    filteringMorsel.resetToFirstRow()

    val reducerArgumentRowIds = new Array[Long](downstreamArgumentReducers.size)
    util.Arrays.fill(reducerArgumentRowIds, INVALID_ARG_ROW_ID) // otherwise we do not decrement for argumentRowId 0
    val reducerDecrementFlags = new util.BitSet(downstreamArgumentReducers.size) // OBS: We interpret "set" as leave the row alone and "unset" as decrement.
    while (filteringMorsel.isValidRow) {
      var j = 0
      while (j < downstreamArgumentReducers.size) {
        val reducer = downstreamArgumentReducers(j)
        val reducerArgumentSlotOffset = reducer.argumentSlotOffset

        // Get and update the reducer argument row id
        val currentReducerArgumentRowId = reducerArgumentRowIds(j)
        val nextReducerArgumentRowId = if (filteringMorsel.isValidRow) filteringMorsel.getArgumentAt(reducerArgumentSlotOffset) else INVALID_ARG_ROW_ID
        reducerArgumentRowIds(j) = nextReducerArgumentRowId

        // If we reached a new argument id at the reducers offset
        if (!filteringMorsel.isValidRow ||
          (currentReducerArgumentRowId != INVALID_ARG_ROW_ID && currentReducerArgumentRowId != nextReducerArgumentRowId)) {

          // If all rows were cancelled
          if (!reducerDecrementFlags.get(j)) {
            reducer.decrement(currentReducerArgumentRowId)
          }
          // Reset flag for the next argument row id
          reducerDecrementFlags.clear(j)
        }

        // If the row is not cancelled, remember to not decrement this reducer
        if (!rowsToCancel.get(filteringMorsel.getCurrentRow)) {
          reducerDecrementFlags.set(j)
        }

        j += 1
      }
      filteringMorsel.moveToNextRow()
    }
    // Any remaining decrement for the last block for each reducer
    var j = 0
    while (j < downstreamArgumentReducers.size) {
      val reducer = downstreamArgumentReducers(j)

      if (!reducerDecrementFlags.get(j) && reducerArgumentRowIds(j) != INVALID_ARG_ROW_ID) {
        reducer.decrement(reducerArgumentRowIds(j))
      }
      j += 1
    }
  }

  private def determineCancelledRows(filteringMorsel: FilteringPipelinedExecutionContext): java.util.BitSet = {
    val rowsToCancel =  new java.util.BitSet(filteringMorsel.maxNumberOfRows)
    while (filteringMorsel.isValidRow) {
      var i = 0
      var isCancelled = false
      var cancellerArgumentRowId: Long = -INVALID_ARG_ROW_ID
      var cancellerArgumentSlotOffset: Int = INVALID_ARG_SLOT_OFFSET

      // Determine if the current row is cancelled by any canceller
      while (i < workCancellers.size && !isCancelled) {
        val cancellerASM = cancellerASMs(i)
        cancellerArgumentRowId = filteringMorsel.getArgumentAt(cancellerASM.argumentSlotOffset)
        if (cancellerASM.peek(cancellerArgumentRowId).isCancelled) {
          cancellerArgumentSlotOffset = cancellerASM.argumentSlotOffset
          isCancelled = true // break
        }
        i += 1
      }
      if (isCancelled) {
        // Cancel all rows up to the next argument id at the slot offset of the canceller
        do {
          // Remember which rows to cancel
          rowsToCancel.set(filteringMorsel.getCurrentRow)
          filteringMorsel.moveToNextRow()
        } while (filteringMorsel.isValidRow && filteringMorsel.getArgumentAt(cancellerArgumentSlotOffset) == cancellerArgumentRowId)

      } else {
        filteringMorsel.moveToNextRow()
      }
    }
    rowsToCancel
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
