/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder

/**
 * Extension of Morsel buffer that also holds an argument state map in order to track
 * argument rows that do not result in any output rows, i.e. gets filtered out.
 *
 * This buffer sits between two pipelines.
 */
abstract class BaseArgExistsMorselBuffer[PRODUCES <: AnyRef, S <: ArgumentState](
                                                             id: BufferId,
                                                             tracker: QueryCompletionTracker,
                                                             downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
                                                             override val argumentStateMaps: ArgumentStateMaps,
                                                             val argumentStateMapId: ArgumentStateMapId
                                                            )
  extends ArgumentCountUpdater
  with AccumulatingBuffer
  with Sink[IndexedSeq[PerArgument[Morsel]]]
  with ClosingSource[PRODUCES]
  with DataHolder {

  protected val argumentStateMap: ArgumentStateMap[S] =
    argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[S]]

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  override final def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel with initial count $initialCount")
    }
    val argumentRowIdsForReducers: Array[Long] = forAllArgumentReducersAndGetArgumentRowIds(downstreamArgumentReducers, argumentMorsel, _.increment(_))
    argumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers, initialCount)
    tracker.increment()
  }

  override final def increment(argumentRowId: Long): Unit = {
    argumentStateMap.increment(argumentRowId)
  }

  override final def decrement(argumentRowId: Long): Unit = {
    argumentStateMap.decrement(argumentRowId)
  }

  override final def clearAll(): Unit = {
    argumentStateMap.clearAll(clearArgumentState)
  }

  protected def clearArgumentState(s: S): Unit

  def filterCancelledArguments(accumulator: MorselAccumulator[_]): Boolean = {
    false
  }

  override def toString: String =
    s"BaseArgExistsMorselBuffer(planId: $argumentStateMapId)$argumentStateMap"
}

// --- Messaging protocol ---

/**
 * Some Morsels for one argument row id. Depending on the [[ArgumentStream]] there might be more data for this argument row id.
 * @param viewOfArgumentRow the argument row for the id, as obtained from the [[MorselApplyBuffer]]
 */
case class MorselData(morsels: IndexedSeq[Morsel],
                      argumentStream: ArgumentStream,
                      argumentRowIdsForReducers: Array[Long],
                      viewOfArgumentRow: MorselRow)

trait ArgumentStream
trait EndOfStream extends ArgumentStream

/**
 * The end of data for one argument row id, when there was actually no data (i.e. everything was filtered out).
 */
case object EndOfEmptyStream extends EndOfStream

/**
 * The end of data for one argument row id, when there was data.
 */
case object EndOfNonEmptyStream extends EndOfStream

/**
 * There will be more data for this argument row id.
 */
case object NotTheEnd extends ArgumentStream
