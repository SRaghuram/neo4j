/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactoryFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

/**
 * An eager version of [[MorselBuffer]] which accumulates all morsels before they become available as input for
 * the next pipeline
 *
 * It requires a top-level argument state (in a singleton argument state map) to become completed before
 * morsel become available for take
 *
 * @param inner inner buffer to delegate real buffer work to
 */
class EagerMorselBuffer(id: BufferId,
                        tracker: QueryCompletionTracker,
                        downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                        workCancellers: ReadOnlyArray[ArgumentStateMapId],
                        argumentStateMaps: ArgumentStateMaps,
                        inner: Buffer[Morsel],
                        val operatorId: Id,
                        val argumentStateMapId: ArgumentStateMapId,
                        argumentStateMapCreator: ArgumentStateMapCreator,
                        stateFactory: StateFactory)
  extends MorselBuffer(id, tracker, downstreamArgumentReducers, workCancellers, argumentStateMaps, inner)
  with AccumulatingBuffer
{
  private val argumentStateMap: ArgumentStateMap[EagerArgumentState] = argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[EagerArgumentState]]

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  // State to flip when ready for take
  private[this] var _hasCompleted: Boolean = false // This should only change once to true, when we take the completed state from the argument state map
  @volatile private[this] var _hasCompletedVolatile: Boolean = false // This should change only once at the same time as _hasCompleted, but provides visibility to other threads in parallel execution

  //---------------------------------------------------------------------------
  // AccumulatingBuffer
  //---------------------------------------------------------------------------
  override def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit = {
    TopLevelArgument.assertTopLevelArgument(argumentRowId) // This is a requirement since we never reset the state

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
    if (argumentStateMap.decrement(argumentRowId) != null) {
      readyForTake()
    }
  }

  //---------------------------------------------------------------------------
  // Sink
  //---------------------------------------------------------------------------
  /**
   * The inner buffer of [[MorselBuffer]] (i.e. [[StandardBuffer]] or [[ConcurrentBuffer]]) has a size limit
   * It is very important that we override this since an eager buffer _needs_ to be able to accumulate every morsel
   */
  override def canPut: Boolean = true

  /**
   * This is essentially the same as [[put]], except that no argument counts are incremented.
   * The reason is that if this is one of the delegates of a [[MorselApplyBuffer]], that
   * buffer took care of incrementing the right ones already.
   */
  override def putInDelegate(morsel: Morsel, resources: QueryResources): Unit = {
    throw new UnsupportedOperationException("EagerBuffer does not support being a delegate")
  }

  //---------------------------------------------------------------------------
  // Source
  //---------------------------------------------------------------------------
  override def take(): MorselParallelizer = {
    if (readyForTake()) {
      super.take()
    } else {
      null
    }
  }

  override def hasData: Boolean = {
    readyForTake() && super.hasData
  }

  private[this] def readyForTake(): Boolean = {
    // NOTE: We do not do `... || _hasCompletedVolatile` in this first check based on the assumption that we expect only a small time window after
    // flipping to ready before `_hasCompleted` becomes visible in parallel (maximum one failed attempt at `argumentStateMap.takeCompleted(1)` per worker)
    // Experimental or formal invalidation is welcome :)
    if (_hasCompleted) {
      true
    } else {
      val completedStates = argumentStateMap.takeCompleted(1)
      if (completedStates != null) {
        // Permanently flip the state to become ready for take
        if (DebugSupport.BUFFERS.enabled) {
          DebugSupport.BUFFERS.log(s"[readyForTake] !!!  $this")
        }
        _hasCompleted = true
        _hasCompletedVolatile = true // This is to also support parallel

        // Release reference counts
        // Alternatively: do it by `completedStates.head.close()`, which has argumentRowIdsForReducers
        forAllArgumentReducers(downstreamArgumentReducers, completedStates.head.argumentRowIdsForReducers, _.decrement(_))
        tracker.decrement() // Decrement the count from initiate /* Count Type: Accumulator Init */
        true
      } else {
        _hasCompletedVolatile // This is to also support parallel
      }
    }
  }

  override def toString: String = s"EagerMorselBuffer($id, $argumentStateMapId, $inner)"
}

object EagerArgumentStateFactory extends ArgumentStateFactory[EagerArgumentState] with ArgumentStateFactoryFactory[EagerArgumentState] {
  override def newStandardArgumentState(argumentRowId: Long,
                                        argumentMorsel: MorselReadCursor,
                                        argumentRowIdsForReducers: Array[Long],
                                        memoryTracker: MemoryTracker): EagerArgumentState =
    new EagerArgumentState(argumentRowIdsForReducers)

  override def newConcurrentArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long]): EagerArgumentState =
    new EagerArgumentState(argumentRowIdsForReducers)

  override def createFactory(stateFactory: StateFactory, operatorId: Int): ArgumentStateFactory[EagerArgumentState] = this
}

class EagerArgumentState(override val argumentRowIdsForReducers: Array[Long]) extends ArgumentState {
  override def argumentRowId: Long = TopLevelArgument.VALUE
  override def shallowSize: Long = EagerArgumentState.SHALLOW_SIZE
}

object EagerArgumentState {
  private val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[EagerArgumentState])
}