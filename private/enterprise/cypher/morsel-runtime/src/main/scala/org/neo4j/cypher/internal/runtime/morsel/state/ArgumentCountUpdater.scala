/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.AccumulatingBuffer

/**
  * Helper class for updating argument counts.
  */
abstract class ArgumentCountUpdater {

  /**
    * Get ArgumentStateMaps by ID.
    */
  def argumentStateMaps: ArgumentStateMaps

  // TODO this does not really need to collect the argument row ids into a collection
  // Step 1: It would be enough to have a var with the latest argument row id per downstream.
  // Step 2: Merge with while loop in MorselApplyBuffer
  private def morselLoop(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                         morsel: MorselExecutionContext,
                         operation: (AccumulatingBuffer, Long) => Unit): Unit = {
    var i = 0
    while (i < downstreamAccumulatingBuffers.length) {
      val buffer = downstreamAccumulatingBuffers(i)
      val argumentRowIds = morsel.allArgumentRowIdsFor(buffer.argumentSlotOffset)
      var j = 0
      while (j < argumentRowIds.size) {
        operation(buffer, argumentRowIds(j))
        j += 1
      }
      i += 1
    }
  }

  private def downstreamLoop[T](downstreamAccumulatingBuffers: IndexedSeq[T],
                             morsel: MorselExecutionContext,
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
                                           morsel: MorselExecutionContext): Unit = {
    downstreamLoop[ArgumentStateMapId](argumentStates, morsel, id => argumentStateMaps(id).initiate(argumentRowId, morsel, null))
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
                                             morsel: MorselExecutionContext): Unit = {
    downstreamLoop[AccumulatingBuffer](accumulatingBuffers, morsel, _.initiate(argumentRowId, morsel))
  }

  /**
    * Apply function on each accumulating buffer, and returns the argument row id for each of them.
    *
    * @param accumulatingBuffers buffers to apply function to
    * @param morsel must point at the row of `argumentRowId`
    * @param fun function to invoke on buffers
    * @return array of argument row ids
    */
  protected def forAllArgumentReducersAndGetArgumentRowIds(accumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                                           morsel: MorselExecutionContext,
                                                           fun: (AccumulatingBuffer, Long) => Unit): Array[Long] = {
    val argumentRowIdsForReducers: Array[Long] = new Array[Long](accumulatingBuffers.size)
    var i = 0
    while (i < accumulatingBuffers.length) {
      val reducer = accumulatingBuffers(i)
      val offset = reducer.argumentSlotOffset
      val argumentRowIdForReducer = morsel.getLongAt(offset)
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

  // ----

  protected def incrementArgumentCounts(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        morsel: MorselExecutionContext): Unit = {
    morselLoop(downstreamAccumulatingBuffers, morsel, _.increment(_))
  }

  protected def decrementArgumentCounts(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        morsel: MorselExecutionContext): Unit = {
    morselLoop(downstreamAccumulatingBuffers, morsel, _.decrement(_))
  }

  protected def decrementArgumentCounts(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        argumentRowIds: IndexedSeq[Long]): Unit = {
    argumentCountLoop(downstreamAccumulatingBuffers, argumentRowIds, _.decrement(_))
  }
}
