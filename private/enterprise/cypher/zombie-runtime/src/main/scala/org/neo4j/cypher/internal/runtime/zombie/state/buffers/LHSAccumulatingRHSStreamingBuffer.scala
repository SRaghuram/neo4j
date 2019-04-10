/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, PipelineId}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.Zombie
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateMaps, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffers.{AccumulatingBuffer, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.LHSAccumulatingRHSStreamingBuffer.RHSBuffer
import org.neo4j.cypher.internal.runtime.zombie.state.{ArgumentCountUpdater, ArgumentStateMap, QueryCompletionTracker, StateFactory}

/**
  * This buffer receives data from two sides. It will accumulate the data from the LHS using an [[ArgumentStateMap]].
  * The data from the RHS is dealt with in a streaming fashion, but still grouped by argument id.
  * The buffer has data available when, for an argument id, all the LHS data is accumulated and some RHS data is available.
  *
  * After all RHS data has arrived and was processed, both the LHS and the RHS state in the ArgumentStateMaps are cleaned up.
  * This Buffer usually sits before a hash join.
  *
  * This buffer is also known as Mr Buff
  * ..................,#####,
  * ..................#_..._#
  * ..................|e`.`e|
  * ..................|..u..|
  * ..................\..=../
  * ..................|\___/|
  * .........___.____/:.....:\____.___
  * ........'...`.-===-\.../-===-.`...'.
  * ....../.......-"""""-.-"""""-.......\
  * ...../'.............=:=.............'\
  * ....'..'..:....o...-=:=-...o....:..'..`.
  * ...(.'.../'..'-.....-'-.....-'..'\...'.)
  * .../'.._/..."......--:--......"...\_..'\
  * ..|...'|......"...---:---..."......|'...|
  * ..|..:.|.......|..---:---..|.......|.:..|
  * ...\.:.|.......|_____._____|.......|.:./
  * .../...(.......|----|------|.......)...\
  * ../.....|......|....|......|......|.....\
  * .|::::/''...../.....|.......\.....''\::::|
  * .'""""......./'.....L_......`\.......""""'
  * ............/'-.,__/`.`\__..-'\
  * ...........;....../.....\......;
  * ...........:...../.......\.....|
  * ...........|..../.........\....|
  * ...........|`../...........|..,/
  * ...........(._.)...........|.._)
  * ...........|...|...........|...|
  * ...........|___|...........\___|
  * ...........:===|............|==|
  * ............\../............|__|
  * ............/\/\.........../"""`8.__
  * ............|oo|...........\__.//___)
  * ............|==|
  * ............\__/
  **/
class LHSAccumulatingRHSStreamingBuffer[LHS_ACC <: MorselAccumulator](tracker: QueryCompletionTracker,
                                                                      downstreamArgumentReducers: Seq[AccumulatingBuffer],
                                                                      argumentStateMaps: ArgumentStateMaps,
                                                                      val lhsArgumentStateMapId: ArgumentStateMapId,
                                                                      val rhsArgumentStateMapId: ArgumentStateMapId,
                                                                      lhsPipelineId: PipelineId,
                                                                      rhsPipelineId: PipelineId,
                                                                      stateFactory: StateFactory
                                             ) extends ArgumentCountUpdater
                                               with Source[(LHS_ACC, MorselExecutionContext)]
                                               with SinkByOrigin {

  private val lhsArgumentStateMap = argumentStateMaps(lhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[LHS_ACC]]
  private val rhsArgumentStateMap = argumentStateMaps(rhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[RHSBuffer]]
  Zombie.debug(s"Mr Buff LHS ASM: $lhsArgumentStateMapId")
  Zombie.debug(s"Mr Buff RHS ASM: $rhsArgumentStateMapId")

  override def sinkFor(fromPipeline: PipelineId): Sink[MorselExecutionContext] =
    if (fromPipeline == lhsPipelineId) {
      LHSSink
    } else if (fromPipeline == rhsPipelineId) {
      RHSSink
    } else {
      throw new IllegalStateException(s"Requesting sink from unexpected pipeline.")
    }

  // TODO optimization: if RHS has completed with no rows, cancel building of the accumulator and vice versa

  override def hasData: Boolean = {
    lhsArgumentStateMap.peekCompleted().exists(lhsAcc => {
      val rhsArgumentState = rhsArgumentStateMap.peek(lhsAcc.argumentRowId)
      rhsArgumentState != null && rhsArgumentState.buffer.hasData
    })
  }

  override def take(): (LHS_ACC, MorselExecutionContext) = {
    val it = lhsArgumentStateMap.peekCompleted()
    while (it.hasNext) {
      val lhsAcc = it.next()
      val rhsBuffer = rhsArgumentStateMap.peek(lhsAcc.argumentRowId)
      if (rhsBuffer != null) {
        val rhsMorsel = rhsBuffer.buffer.take()
        if (rhsMorsel != null) {
          return (lhsAcc, rhsMorsel)
        }
      }
    }
    null
  }

  /**
    * Remove all rows related to cancelled argumentRowIds from `morsel`.
    * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
    *
    * @param rhsMorsel   the input morsel from the RHS
    * @param accumulator the accumulator
    * @return `true` iff both the morsel and the accumulator are cancelled
    */
  def filterCancelledArguments(accumulator: LHS_ACC, rhsMorsel: MorselExecutionContext): Boolean = {
    // TODO
    false
  }

  def close(accumulator: LHS_ACC, rhsMorsel: MorselExecutionContext): Unit = {
    // Check if the argument count is zero -- in the case of the RHS, that means that no more data will ever arrive
    if (rhsArgumentStateMap.hasCompleted(accumulator.argumentRowId)) {
      val rhsAcc = rhsArgumentStateMap.peek(accumulator.argumentRowId)
      // All data from the RHS has arrived
      // If all data is processed and we are the first thread to remove the RHS accumulator (there can be a race here)
      if (rhsAcc != null && !rhsAcc.buffer.hasData && rhsArgumentStateMap.remove(accumulator.argumentRowId)) {
        // Clean up the LHS as well
        lhsArgumentStateMap.remove(accumulator.argumentRowId)
      }
    }
    // Decrement for a morsel in the RHS buffer
    decrementArgumentCounts(downstreamArgumentReducers, Seq(accumulator.argumentRowId))
    tracker.decrement()
  }

  // The LHS does not need any reference counting, because no tasks
  // will ever be produced from the LHS
  object LHSSink extends Sink[MorselExecutionContext] with AccumulatingBuffer {
    override val argumentSlotOffset: Int = lhsArgumentStateMap.argumentSlotOffset

    override def put(morsel: MorselExecutionContext): Unit = {
      morsel.resetToFirstRow()
      lhsArgumentStateMap.update(morsel, (acc, morselView) => acc.update(morselView))
    }

    override def initiate(argumentRowId: Long): Unit = {
      lhsArgumentStateMap.initiate(argumentRowId)
    }

    override def increment(argumentRowId: Long): Unit = {
      lhsArgumentStateMap.increment(argumentRowId)
    }

    override def decrement(argumentRowId: Long): Unit = {
      lhsArgumentStateMap.decrement(argumentRowId)
    }
  }

  // We need to reference count both tasks and argument IDs on the RHS.
  // Tasks need to be tracked since the RHS accumulator's Buffer is used multiple times
  // to spawn tasks, unlike in the MorselArgumentStateBuffer where you only take the accumulator once.
  object RHSSink extends Sink[MorselExecutionContext] with AccumulatingBuffer {
    override val argumentSlotOffset: Int = rhsArgumentStateMap.argumentSlotOffset

    override def put(morsel: MorselExecutionContext): Unit = {
      morsel.resetToFirstRow()
      // there is no need to take a lock in this case, because we are sure the argument state is thread safe when needed (is created by state factory)
      rhsArgumentStateMap.update(morsel, (acc, morselView) => {
        acc.buffer.put(morselView)
        // Increment for a morsel in the RHS buffer
        incrementArgumentCounts(downstreamArgumentReducers, Seq(acc.argumentRowId))
        tracker.increment()
      }, takeLock = false)
    }

    override def initiate(argumentRowId: Long): Unit = {
      rhsArgumentStateMap.initiate(argumentRowId)
      // Increment for an ArgumentID in RHS's accumulator
      incrementArgumentCounts(downstreamArgumentReducers, Seq(argumentRowId))
      tracker.increment()
    }

    override def increment(argumentRowId: Long): Unit = {
      rhsArgumentStateMap.increment(argumentRowId)
    }

    override def decrement(argumentRowId: Long): Unit = {
      val isZero = rhsArgumentStateMap.decrement(argumentRowId)
      if (isZero) {
        // Decrement for an ArgumentID in RHS's accumulator
        decrementArgumentCounts(downstreamArgumentReducers, Seq(argumentRowId))
        tracker.decrement()
      }
    }
  }

}

object LHSAccumulatingRHSStreamingBuffer {

  /**
    * The buffer used on the RHS input. We can use a [[StandardBuffer]] since we protect
    * all concurrent access to it through the [[ArgumentStateMap]].
    */
  class RHSBuffer(override val argumentRowId: Long,
                  private[LHSAccumulatingRHSStreamingBuffer] val buffer: Buffer[MorselExecutionContext]) extends ArgumentState {
  }

}
