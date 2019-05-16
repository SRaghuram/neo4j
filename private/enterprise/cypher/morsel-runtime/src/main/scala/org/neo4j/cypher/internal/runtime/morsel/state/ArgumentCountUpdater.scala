/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.AccumulatingBuffer

/**
  * Helper class for updating argument counts.
  */
abstract class ArgumentCountUpdater {

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

  protected def initiateArgumentReducers(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                         morsel: MorselExecutionContext): Unit = {
    morselLoop(downstreamAccumulatingBuffers, morsel, _.initiate(_))
  }

  protected def incrementArgumentCounts(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        morsel: MorselExecutionContext): Unit = {
    morselLoop(downstreamAccumulatingBuffers, morsel, _.increment(_))
  }

  protected def decrementArgumentCounts(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        morsel: MorselExecutionContext): Unit = {
    morselLoop(downstreamAccumulatingBuffers, morsel, _.decrement(_))
  }

  protected def incrementArgumentCounts(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        argumentRowIds: IndexedSeq[Long]): Unit = {
    argumentCountLoop(downstreamAccumulatingBuffers, argumentRowIds, _.increment(_))
  }

  protected def decrementArgumentCounts(downstreamAccumulatingBuffers: IndexedSeq[AccumulatingBuffer],
                                        argumentRowIds: IndexedSeq[Long]): Unit = {
    argumentCountLoop(downstreamAccumulatingBuffers, argumentRowIds, _.decrement(_))
  }
}
