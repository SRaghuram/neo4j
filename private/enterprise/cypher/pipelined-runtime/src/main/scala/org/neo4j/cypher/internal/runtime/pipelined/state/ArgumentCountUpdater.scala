/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer

/**
 * Helper class for updating argument counts.
 */
abstract class ArgumentCountUpdater {

  /**
   * Get ArgumentStateMaps by ID.
   */
  def argumentStateMaps: ArgumentStateMaps

  private def morselLoop(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                         morsel: Morsel,
                         operation: (AccumulatingBuffer, Long) => Unit): Unit = {

    val lastSeenRowIds = new Array[Long](downstreamAccumulatingBuffers.size)
    // Write all initial last seen ids to -1
    util.Arrays.fill(lastSeenRowIds, -1L)

    var i = 0
    val cursor = morsel.readCursor()
    while (cursor.next()) {
      i = 0
      while (i < downstreamAccumulatingBuffers.length) {
        val buffer = downstreamAccumulatingBuffers(i)
        val currentRowId = ArgumentSlots.getArgumentAt(cursor, buffer.argumentSlotOffset)
        if (currentRowId != lastSeenRowIds(i)) {
          operation(buffer, currentRowId)
          lastSeenRowIds(i) = currentRowId
        }
        i += 1
      }
    }
  }

  private def downstreamLoop[T](downstreamAccumulatingBuffers: IndexedSeq[T],
                                operation: T => Unit): Unit = {
    var i = 0
    while (i < downstreamAccumulatingBuffers.length) {
      val buffer = downstreamAccumulatingBuffers(i)
      operation(buffer)
      i += 1
    }
  }

  private def argumentCountLoop(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                argumentRowIds: IndexedSeq[Long],
                                operation: (AccumulatingBuffer, Long) => Unit): Unit = {
    var i = 0
    while (i < downstreamAccumulatingBuffers.length) {
      val buffer = downstreamAccumulatingBuffers(i)
      var j = 0
      while (j < argumentRowIds.size) {
        operation(buffer, argumentRowIds(j))
        j += 1
      }
      i += 1
    }
  }

  /**
   * Initiates argument state maps.
   *
   * In practice, this is only called for Limit.
   *
   * @param argumentStates all argument state maps must be at the same apply nesting level
   * @param argumentRowId argument row at the same apply nesting level as the argument states expect
   * @param morsel must point at the row of `argumentRowId`
   */
  protected def initiateArgumentStatesHere(argumentStates: IndexedSeq[ArgumentStateMapId],
                                           argumentRowId: Long,
                                           morsel: MorselReadCursor): Unit = {
    downstreamLoop[ArgumentStateMapId](argumentStates, id => argumentStateMaps(id).initiate(argumentRowId, morsel, null))
  }

  /**
   * Initiates accumulating buffers.
   *
   * @param accumulatingBuffers all argument state maps must be at the same apply nesting level
   * @param argumentRowId argument row at the same apply nesting level as the argument states expect
   * @param morsel must point at the row of `argumentRowId`
   */
  protected def initiateArgumentReducersHere(accumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                             argumentRowId: Long,
                                             morsel: MorselReadCursor): Unit = {
    downstreamLoop[AccumulatingBuffer](accumulatingBuffers, _.initiate(argumentRowId, morsel))
  }

  /**
   * Apply function on each accumulating buffer, and returns the argument row id for each of them.
   *
   * @param accumulatingBuffers buffers to apply function to
   * @param morselRow must point at the row of `argumentRowId`
   * @param fun function to invoke on buffers
   * @return array of argument row ids
   */
  protected def forAllArgumentReducersAndGetArgumentRowIds(accumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                                           morselRow: MorselReadCursor,
                                                           fun: (AccumulatingBuffer, Long) => Unit): Array[Long] = {
    val argumentRowIdsForReducers: Array[Long] = new Array[Long](accumulatingBuffers.size)
    var i = 0
    while (i < accumulatingBuffers.length) {
      val reducer = accumulatingBuffers(i)
      val offset = reducer.argumentSlotOffset
      val argumentRowIdForReducer = ArgumentSlots.getArgumentAt(morselRow, offset)
      argumentRowIdsForReducers(i) = argumentRowIdForReducer
      fun(reducer, argumentRowIdForReducer)
      i += 1
    }
    argumentRowIdsForReducers
  }

  /**
   * Apply function on each accumulating buffer
   *
   * @param accumulatingBuffers buffers to apply function to
   * @param argumentRowIds argument row ids for the provided buffers, buffers may be at different apply nesting levels
   * @param fun function to invoke on buffers
   */
  protected def forAllArgumentReducers(accumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                       argumentRowIds: Array[Long],
                                       fun: (AccumulatingBuffer, Long) => Unit ): Unit = {
    var i = 0
    while (i < accumulatingBuffers.length) {
      val reducer = accumulatingBuffers(i)
      val argumentRowIdForReducer = argumentRowIds(i)
      fun(reducer, argumentRowIdForReducer)
      i += 1
    }
  }

  /**
   * Increment each accumulating buffer for all argument row id in the given morsel.
   */
  protected def incrementArgumentCounts(accumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        morsel: Morsel): Unit = {
    morselLoop(accumulatingBuffers, morsel, _.increment(_))
  }

  /**
   * Decrement each accumulating buffer for all argument row id in the given morsel.
   */
  protected def decrementArgumentCounts(accumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        morsel: Morsel): Unit = {
    morselLoop(accumulatingBuffers, morsel, _.decrement(_))
  }

  /**
   * Decrement each accumulating buffer for all given argument row ids
   */
  protected def decrementArgumentCounts(accumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        argumentRowIds: IndexedSeq[Long]): Unit = {
    argumentCountLoop(accumulatingBuffers, argumentRowIds, _.decrement(_))
  }
}
