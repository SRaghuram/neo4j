/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.Initialization
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
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

  private def morselLoop(downstreamAccumulatingBuffers: ReadOnlyArray[AccumulatingBuffer],
                         morsel: Morsel,
                         operation: (AccumulatingBuffer, Long) => Unit): Unit = {

    val lastSeenRowIds = new Array[Long](downstreamAccumulatingBuffers.length)
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

  private def argumentCountLoop(downstreamAccumulatingBuffers: ReadOnlyArray[AccumulatingBuffer],
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
   * In practice, this is only called for Distinct.
   *
   * @param argumentStates all argument state maps must be at the same apply nesting level
   * @param argumentRowId argument row at the same apply nesting level as the argument states expect
   * @param morsel must point at the row of `argumentRowId`
   */
  protected def initiateArgumentStatesHere(argumentStates: ReadOnlyArray[ArgumentStateMapId],
                                           argumentRowId: Long,
                                           morsel: MorselReadCursor): Unit = {
    argumentStates.foreach {
      // We pass in an initialCount of 1, but it actually does not matter at all what we pass in since
      // argument states use `ImmutableStateController`s which don't keep a current count since
      // the count would never be updated or queried.
      id => argumentStateMaps(id).initiate(argumentRowId, morsel, null, 1)
    }
  }

  /**
   * Initiates argument state maps for work cancellers.
   *
   * In practice, this is only called for Limit/Skip.
   *
   * @param argumentStates all argument state maps must be at the same apply nesting level
   * @param argumentRowId  argument row at the same apply nesting level as the argument states expect
   * @param morsel         must point at the row of `argumentRowId`
   */
  protected def initiateWorkCancellerArgumentStatesHere(argumentStates: ReadOnlyArray[Initialization[ArgumentStateMapId]],
                                                        argumentRowId: Long,
                                                        morsel: MorselReadCursor): Unit = {
    argumentStates.foreach {
      case Initialization(id, initialCount) => argumentStateMaps(id).initiate(argumentRowId, morsel, null, initialCount)
    }
  }

  /**
   * Initiates accumulating buffers.
   *
   * @param accumulatingBuffers all argument state maps must be at the same apply nesting level
   * @param argumentRowId argument row at the same apply nesting level as the argument states expect
   * @param morsel must point at the row of `argumentRowId`
   */
  protected def initiateArgumentReducersHere(accumulatingBuffers: ReadOnlyArray[Initialization[AccumulatingBuffer]],
                                             argumentRowId: Long,
                                             morsel: MorselReadCursor): Unit = {
    accumulatingBuffers.foreach {
      case Initialization(buffer, initialCount) => buffer.initiate(argumentRowId, morsel, initialCount)
    }
  }

  /**
   * Apply function on each accumulating buffer, and returns the argument row id for each of them.
   *
   * @param accumulatingBuffers buffers to apply function to
   * @param morselRow must point at the row of `argumentRowId`
   * @param fun function to invoke on buffers
   * @return array of argument row ids
   */
  protected def forAllArgumentReducersAndGetArgumentRowIds(accumulatingBuffers: ReadOnlyArray[AccumulatingBuffer],
                                                           morselRow: MorselReadCursor,
                                                           fun: (AccumulatingBuffer, Long) => Unit): Array[Long] = {
    val argumentRowIdsForReducers: Array[Long] = new Array[Long](accumulatingBuffers.length)
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
  protected def forAllArgumentReducers(accumulatingBuffers: ReadOnlyArray[AccumulatingBuffer],
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
  protected def incrementArgumentCounts(accumulatingBuffers: ReadOnlyArray[AccumulatingBuffer],
                                        morsel: Morsel): Unit = {
    morselLoop(accumulatingBuffers, morsel, _.increment(_))
  }

  /**
   * Decrement each accumulating buffer for all argument row id in the given morsel.
   */
  protected def decrementArgumentCounts(accumulatingBuffers: ReadOnlyArray[AccumulatingBuffer],
                                        morsel: Morsel): Unit = {
    morselLoop(accumulatingBuffers, morsel, _.decrement(_))
  }

  /**
   * Decrement each accumulating buffer for all given argument row ids
   */
  protected def decrementArgumentCounts(accumulatingBuffers: ReadOnlyArray[AccumulatingBuffer],
                                        argumentRowIds: IndexedSeq[Long]): Unit = {
    argumentCountLoop(accumulatingBuffers, argumentRowIds, _.decrement(_))
  }
}
