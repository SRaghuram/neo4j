/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, PipelineId}
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateMaps, MorselAccumulator, PerArgument}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.{AccumulatingBuffer, DataHolder, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentCountUpdater, ArgumentStateMap, QueryCompletionTracker}

/**
  * Morsel buffer that groups incoming rows by argumentRowId by delegating to an [[ArgumentStateMap]].
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
                                    with SinkByOrigin
                                    with DataHolder {

  private val argumentStateMap: ArgumentStateMap[ACC] = argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[ACC]]

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  override def sinkFor[T <: AnyRef](fromPipeline: PipelineId): Sink[T] = this.asInstanceOf[Sink[T]]

  override def put(data: IndexedSeq[PerArgument[DATA]]): Unit = {
    DebugSupport.logBuffers(s"[put]   $this <- ${data.mkString(", ")}")
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
      DebugSupport.logBuffers(s"[take]  $this -> $accumulator")
    }
    accumulator
  }

  override def initiate(argumentRowId: Long, argumentMorsel: MorselExecutionContext): Unit = {
    DebugSupport.logBuffers(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel")

    // TODO consider refactoring this into ArgumentCountUpdater
    val argumentRowIdsForReducers: Array[Long] = new Array[Long](downstreamArgumentReducers.size)
    var i = 0
    while (i < downstreamArgumentReducers.length) {
      val reducer = downstreamArgumentReducers(i)
      val offset = reducer.argumentSlotOffset
      val argumentRowIdForReducer = argumentMorsel.getLongAt(offset)
      argumentRowIdsForReducers(i) = argumentRowIdForReducer

      // Increment the downstream reducer for its argument row id
      reducer.increment(argumentRowIdForReducer)

      i += 1
    }

    argumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers)

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
    DebugSupport.logBuffers(s"[close] $this -X- $accumulator")

    // TODO consider refactoring this into ArgumentCountUpdater
    val argumentRowIdsForReducers: Array[Long] = accumulator.argumentRowIdsForReducers
    var i = 0
    while (i < downstreamArgumentReducers.length) {
      val reducer = downstreamArgumentReducers(i)
      val argumentRowIdForReducer = argumentRowIdsForReducers(i)

      reducer.decrement(argumentRowIdForReducer)

      i += 1
    }

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
