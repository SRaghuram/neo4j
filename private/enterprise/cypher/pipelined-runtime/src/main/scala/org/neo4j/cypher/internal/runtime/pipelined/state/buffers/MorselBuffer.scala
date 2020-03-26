/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import java.util

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.FilteringMorsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselBuffer.INVALID_ARG_ROW_ID
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselBuffer.INVALID_ARG_SLOT_OFFSET

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
                   inner: Buffer[Morsel]
                  ) extends ArgumentCountUpdater
                    with Sink[Morsel]
                    with Source[MorselParallelizer]
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

  override def put(morsel: Morsel): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- $morsel")
    }
    if (morsel.hasData) {
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
  def putInDelegate(morsel: Morsel): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[putInDelegate] $this <- $morsel")
    }
    if (morsel.hasData) {
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
  def filterCancelledArguments(morsel: Morsel): Boolean = {
    if (workCancellers.nonEmpty) {
      if (!morsel.isInstanceOf[FilteringMorsel])
        throw new IllegalArgumentException(s"Expected filtering morsel for filterCancelledArguments in buffer $id, but got ${morsel.getClass}")

      val filteringMorsel = morsel.asInstanceOf[FilteringMorsel]

      val rowsToCancel = determineCancelledRows(filteringMorsel)

      // Second loop: decrement for reducers
      if(!rowsToCancel.isEmpty) {
        decrementReducers(filteringMorsel, rowsToCancel)
        // Actually cancel all rows
        filteringMorsel.cancelRows(rowsToCancel)
      }
    }

    morsel.isEmpty
  }

  private def decrementReducers(filteringMorsel: FilteringMorsel, rowsToCancel: util.BitSet): Unit = {
    val cursor = filteringMorsel.readCursor()

    val reducerArgumentRowIds = new Array[Long](downstreamArgumentReducers.size)
    util.Arrays.fill(reducerArgumentRowIds, INVALID_ARG_ROW_ID) // otherwise we do not decrement for argumentRowId 0
    val reducerDecrementFlags = new util.BitSet(downstreamArgumentReducers.size) // OBS: We interpret "set" as leave the row alone and "unset" as decrement.
    while (cursor.next()) {
      var j = 0
      while (j < downstreamArgumentReducers.size) {
        val reducer = downstreamArgumentReducers(j)
        val reducerArgumentSlotOffset = reducer.argumentSlotOffset

        // Get and update the reducer argument row id
        val currentReducerArgumentRowId = reducerArgumentRowIds(j)
        val nextReducerArgumentRowId = if (cursor.onValidRow) ArgumentSlots.getArgumentAt(cursor, reducerArgumentSlotOffset) else INVALID_ARG_ROW_ID
        reducerArgumentRowIds(j) = nextReducerArgumentRowId

        // If we reached a new argument id at the reducers offset
        if (!cursor.onValidRow ||
          (currentReducerArgumentRowId != INVALID_ARG_ROW_ID && currentReducerArgumentRowId != nextReducerArgumentRowId)) {

          // If all rows were cancelled
          if (!reducerDecrementFlags.get(j)) {
            reducer.decrement(currentReducerArgumentRowId)
          }
          // Reset flag for the next argument row id
          reducerDecrementFlags.clear(j)
        }

        // If the row is not cancelled, remember to not decrement this reducer
        if (!rowsToCancel.get(cursor.row)) {
          reducerDecrementFlags.set(j)
        }

        j += 1
      }
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

  private def determineCancelledRows(filteringMorsel: FilteringMorsel): java.util.BitSet = {
    val rowsToCancel =  new java.util.BitSet(filteringMorsel.maxNumberOfRows)
    val cursor = filteringMorsel.readCursor(onFirstRow = true)
    while (cursor.onValidRow()) {
      var i = 0
      var isCancelled = false
      var cancellerArgumentRowId: Long = -INVALID_ARG_ROW_ID
      var cancellerArgumentSlotOffset: Int = INVALID_ARG_SLOT_OFFSET

      // Determine if the current row is cancelled by any canceller
      while (i < workCancellers.size && !isCancelled) {
        val cancellerASM = cancellerASMs(i)
        cancellerArgumentRowId = ArgumentSlots.getArgumentAt(cursor, cancellerASM.argumentSlotOffset)
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
          rowsToCancel.set(cursor.row)
          cursor.next()
        } while (cursor.onValidRow && ArgumentSlots.getArgumentAt(cursor, cancellerArgumentSlotOffset) == cancellerArgumentRowId)

      } else {
        cursor.next()
      }
    }
    rowsToCancel
  }

  /**
   * Decrement reference counters attached to `morsel`.
   */
  def close(morsel: Morsel): Unit = {
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
  class Parallelizer(original: Morsel) extends MorselParallelizer {
    private var usedOriginal = false

    override def originalForClosing: Morsel = original

    override def nextCopy: Morsel = {
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
