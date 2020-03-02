/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.UnorderedArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder

/**
 * Morsel buffer that groups incoming rows by argumentRowId by delegating to an [[org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap]].
 *
 * This buffer sits before a pipeline that starts with a reducer. It accumulates the state into the
 * ArgumentStateMap.
 *
 * @param argumentStateMapId id of the argument state map to reduce data into.
 */
class MorselArgumentStateBuffer[DATA <: AnyRef,
  ACC <: MorselAccumulator[DATA]
](
   tracker: QueryCompletionTracker,
   downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
   override val argumentStateMaps: ArgumentStateMaps,
   val argumentStateMapId: ArgumentStateMapId
 ) extends ArgumentCountUpdater
   with AccumulatingBuffer
   with Sink[IndexedSeq[PerArgument[DATA]]]
   with Source[ACC]
   with DataHolder {

  private val argumentStateMap: UnorderedArgumentStateMap[ACC] = argumentStateMaps(argumentStateMapId).asInstanceOf[UnorderedArgumentStateMap[ACC]]

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  override def put(data: IndexedSeq[PerArgument[DATA]]): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
    }
    var i = 0
    while (i < data.length) {
      argumentStateMap.update(data(i).argumentRowId, acc => acc.update(data(i).value))
      i += 1
    }
  }

  override def canPut: Boolean = true

  override def hasData: Boolean = argumentStateMap.hasCompleted

  override def take(): ACC = {
    val accumulator = argumentStateMap.takeOneCompleted()
    if (accumulator != null) {
      if (DebugSupport.BUFFERS.enabled) {
        DebugSupport.BUFFERS.log(s"[take]  $this -> $accumulator")
      }
    }
    accumulator
  }

  override def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel with initial count $initialCount")
    }

    val argumentRowIdsForReducers: Array[Long] = forAllArgumentReducersAndGetArgumentRowIds(downstreamArgumentReducers, argumentMorsel, _.increment(_))
    argumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers, initialCount)

    tracker.increment()
  }

  override def increment(argumentRowId: Long): Unit = {
    argumentStateMap.increment(argumentRowId)
  }

  override def decrement(argumentRowId: Long): Unit = {
    argumentStateMap.decrement(argumentRowId)
  }

  override def clearAll(): Unit = {
    argumentStateMap.clearAll(acc => close(acc))
  }

  /**
   * Decrement reference counters attached to `accumulator`.
   */
  def close(accumulator: MorselAccumulator[_]): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[close] $this -X- $accumulator")
    }
    forAllArgumentReducers(downstreamArgumentReducers, accumulator.argumentRowIdsForReducers, _.decrement(_))
    tracker.decrement()
  }

  /**
   * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
   *
   * @param accumulator the accumulator
   * @return `true` iff the accumulator is cancelled
   */
  def filterCancelledArguments(accumulator: MorselAccumulator[_]): Boolean = {
    false
  }

  override def toString: String =
    s"MorselArgumentStateBuffer(planId: $argumentStateMapId)$argumentStateMap"
}
