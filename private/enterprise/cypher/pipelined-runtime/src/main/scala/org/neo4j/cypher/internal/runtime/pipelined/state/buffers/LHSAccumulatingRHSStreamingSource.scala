/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.PipelinedDebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.LHSAccumulatingRHSStreamingSource.removeArgumentStatesIfRhsBufferIsEmpty

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
class LHSAccumulatingRHSStreamingSource[DATA <: AnyRef,
                                        LHS_ACC <: MorselAccumulator[DATA]
                                       ](tracker: QueryCompletionTracker,
                                         downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                                         override val argumentStateMaps: ArgumentStateMaps,
                                         val lhsArgumentStateMapId: ArgumentStateMapId,
                                         val rhsArgumentStateMapId: ArgumentStateMapId,
                                       ) extends ArgumentCountUpdater
                                            with Source[AccumulatorAndMorsel[DATA, LHS_ACC]]
                                            with DataHolder {

  private val lhsArgumentStateMap = argumentStateMaps(lhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[LHS_ACC]]
  private val rhsArgumentStateMap = argumentStateMaps(rhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[ArgumentStateBuffer]]

  checkOnlyWhenAssertionsAreEnabled {
    tracker.addCompletionAssertion { () =>
      if (lhsArgumentStateMap.exists(_ => true) || rhsArgumentStateMap.exists(_ => true)) {
        throw new RuntimeResourceLeakException("Argument state maps should be empty.")
      }
    }
  }

  // TODO optimization: if RHS has completed with no rows, cancel building of the accumulator and vice versa

  override def hasData: Boolean = {
    lhsArgumentStateMap.peekCompleted().exists(lhsAcc => {
      val rhsArgumentState = rhsArgumentStateMap.peek(lhsAcc.argumentRowId)
      rhsArgumentState != null && rhsArgumentState.hasData
    })
  }

  override def take(): AccumulatorAndMorsel[DATA, LHS_ACC] = {
    val it = lhsArgumentStateMap.peekCompleted()
    while (it.hasNext) {
      val lhsAcc = it.next()
      val rhsBuffer = rhsArgumentStateMap.peek(lhsAcc.argumentRowId)
      if (rhsBuffer != null) {
        val rhsMorsel = rhsBuffer.take()
        if (rhsMorsel != null) {
          if (DebugSupport.BUFFERS.enabled) {
            DebugSupport.BUFFERS.log(s"[take] $this -> $lhsAcc & ${PipelinedDebugSupport.prettyMorselWithHeader("", rhsMorsel).reduce(_ + _)}")
          }
          return AccumulatorAndMorsel(lhsAcc, rhsMorsel)
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
  def filterCancelledArguments(accumulator: MorselAccumulator[_], rhsMorsel: Morsel): Boolean = {
    // TODO
    false
  }

  override def clearAll(): Unit = {
    // Since the RHS controls downstream execution, it is enough to empty the RHS to stop
    // further work from happening. The LHS will be cleared shortly as the query is stopped
    // and left to be garbage collected.
    rhsArgumentStateMap.clearAll(buffer => {
      var morsel = buffer.take()
      while (morsel != null) {
        forAllArgumentReducers(downstreamArgumentReducers, buffer.argumentRowIdsForReducers, _.decrement(_))
        tracker.decrement()
        morsel = buffer.take()
      }
    })
  }

  def close(accumulator: MorselAccumulator[_], rhsMorsel: Morsel): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[close] $this -X- $accumulator & ${PipelinedDebugSupport.prettyMorselWithHeader("", rhsMorsel).reduce(_ + _)}")
    }

    val argumentRowIdsForReducers = accumulator.argumentRowIdsForReducers
    val argumentRowId = accumulator.argumentRowId

    // Check if the argument count is zero -- in the case of the RHS, that means that no more data will ever arrive
    if (rhsArgumentStateMap.hasCompleted(argumentRowId)) {
      val rhsAcc = rhsArgumentStateMap.peek(argumentRowId)
      // All data from the RHS has arrived
      removeArgumentStatesIfRhsBufferIsEmpty(lhsArgumentStateMap, rhsArgumentStateMap, rhsAcc, argumentRowId)
    }
    // Decrement for a morsel in the RHS buffer
    forAllArgumentReducers(downstreamArgumentReducers, argumentRowIdsForReducers, _.decrement(_))
    tracker.decrement()
  }

}

// The LHS does not need any reference counting, because no tasks
// will ever be produced from the LHS
class LHSAccumulatingSink[DATA <: AnyRef, LHS_ACC <: MorselAccumulator[DATA]](val argumentStateMapId: ArgumentStateMapId,
                                                                              downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                                                                              override val argumentStateMaps: ArgumentStateMaps,
                                                                              rhsArgumentStateMapId: ArgumentStateMapId)
  extends ArgumentCountUpdater
  with Sink[IndexedSeq[PerArgument[DATA]]]
  with AccumulatingBuffer {

  private val argumentStateMap = argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[LHS_ACC]]
  private val rhsArgumentStateMap = argumentStateMaps(rhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[ArgumentStateBuffer]]

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  override def toString: String = s"${getClass.getSimpleName}(planId:${argumentStateMapId.x}, $argumentStateMap)"

  override def put(data: IndexedSeq[PerArgument[DATA]], resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
    }
    var i = 0
    while (i < data.length) {
      argumentStateMap.update(data(i).argumentRowId, acc => acc.update(data(i).value, resources))
      i += 1
    }
  }

  override def canPut: Boolean = true

  override def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel with initial count $initialCount")
    }
    val argumentRowIdsForReducers: Array[Long] = forAllArgumentReducersAndGetArgumentRowIds(downstreamArgumentReducers, argumentMorsel, (_, _) => Unit)
    argumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers, initialCount)
  }

  override def increment(argumentRowId: Long): Unit = {
    argumentStateMap.increment(argumentRowId)
  }

  override def decrement(argumentRowId: Long): Unit = {
    if (argumentStateMap.decrement(argumentRowId) != null && rhsArgumentStateMap.hasCompleted(argumentRowId)) {
      val rhsBuf = rhsArgumentStateMap.peek(argumentRowId)

      // If there is no data in RHS buffer, no more tasks would be created for this argumentRowId and
      // it won't be removed by LHSAccumulatingRHSStreamingSource, so we have to remove the RHS buffer
      // and its corresponding LHS accumulator here to not leave any dangling state.
      removeArgumentStatesIfRhsBufferIsEmpty(argumentStateMap, rhsArgumentStateMap, rhsBuf, argumentRowId)
    }
  }
}

// We need to reference count both tasks and argument IDs on the RHS.
// Tasks need to be tracked since the RHS accumulator's Buffer is used multiple times
// to spawn tasks, unlike in the MorselArgumentStateBuffer where you only take the accumulator once.
class RHSStreamingSink(val argumentStateMapId: ArgumentStateMapId,
                       downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                       override val argumentStateMaps: ArgumentStateMaps,
                       tracker: QueryCompletionTracker,
                       lhsArgumentStateMapId: ArgumentStateMapId) extends ArgumentCountUpdater
                                                                  with Sink[IndexedSeq[PerArgument[Morsel]]]
                                                                  with AccumulatingBuffer {
  private val argumentStateMap = argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[ArgumentStateBuffer]]
  private val lhsArgumentStateMap = argumentStateMaps(lhsArgumentStateMapId)

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  override def toString: String = s"${getClass.getSimpleName}(planId:${argumentStateMapId.x}, $argumentStateMap)"

  override def put(data: IndexedSeq[PerArgument[Morsel]], resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
    }
    // there is no need to take a lock in this case, because we are sure the argument state is thread safe when needed (is created by state factory)
    var i = 0
    while (i < data.length) {
      val argumentValue = data(i)
      argumentStateMap.update(argumentValue.argumentRowId, acc => {
        acc.put(argumentValue.value, resources)
        // Increment for a morsel in the RHS buffer
        forAllArgumentReducers(downstreamArgumentReducers, acc.argumentRowIdsForReducers, _.increment(_))
      })
      tracker.increment()
      i += 1
    }
  }

  override def canPut: Boolean = true

  override def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel with initial count $initialCount")
    }
    // Increment for an ArgumentID in RHS's accumulator
    val argumentRowIdsForReducers: Array[Long] = forAllArgumentReducersAndGetArgumentRowIds(downstreamArgumentReducers, argumentMorsel, _.increment(_))
    argumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers, initialCount)
    tracker.increment()
  }

  override def increment(argumentRowId: Long): Unit = {
    argumentStateMap.increment(argumentRowId)
  }

  override def decrement(argumentRowId: Long): Unit = {
    val maybeBuffer = argumentStateMap.decrement(argumentRowId)
    if (maybeBuffer != null) {
      if (lhsArgumentStateMap.hasCompleted(argumentRowId)) {
        // If there is no data in the buffer, no more tasks would be created for this argumentRowId and
        // it won't be removed by LHSAccumulatingRHSStreamingSource, so we have to remove the current buffer
        // and its corresponding LHS accumulator here to not leave any dangling state.
        removeArgumentStatesIfRhsBufferIsEmpty(lhsArgumentStateMap, argumentStateMap, maybeBuffer, argumentRowId)
      }

      // Decrement for an ArgumentID in RHS's accumulator
      val argumentRowIdsForReducers = maybeBuffer.argumentRowIdsForReducers
      forAllArgumentReducers(downstreamArgumentReducers, argumentRowIdsForReducers, _.decrement(_))
      tracker.decrement()
    }
  }
}

object LHSAccumulatingRHSStreamingSource {

  def removeArgumentStatesIfRhsBufferIsEmpty(lhsArgumentStateMap: ArgumentStateMap[_ <: ArgumentState],
                                             rhsArgumentStateMap: ArgumentStateMap[ArgumentStateBuffer],
                                             rhsBuffer: ArgumentStateBuffer,
                                             argumentRowId: Long): Unit = {
    // If all data is processed and we are the first thread to remove the RHS accumulator (there can be a race here)
    if (rhsBuffer != null && !rhsBuffer.hasData && rhsArgumentStateMap.remove(argumentRowId) != null) {
      // Clean up the LHS as well
      val lhsAccumulator = lhsArgumentStateMap.remove(argumentRowId)
      if (lhsAccumulator != null)
        lhsAccumulator.close()
    }
  }
}