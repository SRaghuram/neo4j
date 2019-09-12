/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, PipelineId}
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.MorselDebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateMaps, MorselAccumulator, PerArgument}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.{AccumulatingBuffer, AccumulatorAndMorsel, DataHolder, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentCountUpdater, ArgumentStateMap, QueryCompletionTracker, StateFactory}

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
class LHSAccumulatingRHSStreamingBuffer[DATA <: AnyRef,
                                        LHS_ACC <: MorselAccumulator[DATA]
                                       ](tracker: QueryCompletionTracker,
                                         downstreamArgumentReducers: IndexedSeq[AccumulatingBuffer],
                                         override val argumentStateMaps: ArgumentStateMaps,
                                         val lhsArgumentStateMapId: ArgumentStateMapId,
                                         val rhsArgumentStateMapId: ArgumentStateMapId,
                                         lhsPipelineId: PipelineId,
                                         rhsPipelineId: PipelineId,
                                         stateFactory: StateFactory
                                       ) extends ArgumentCountUpdater
                                            with Source[AccumulatorAndMorsel[DATA, LHS_ACC]]
                                            with SinkByOrigin
                                            with DataHolder {

  private val lhsArgumentStateMap = argumentStateMaps(lhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[LHS_ACC]]
  private val rhsArgumentStateMap = argumentStateMaps(rhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[ArgumentStateBuffer]]

  override def sinkFor[T <: AnyRef](fromPipeline: PipelineId): Sink[T] =
    if (fromPipeline == lhsPipelineId) {
      LHSSink.asInstanceOf[Sink[T]]
    } else if (fromPipeline == rhsPipelineId) {
      RHSSink.asInstanceOf[Sink[T]]
    } else {
      throw new IllegalStateException(s"Requesting sink from unexpected pipeline.")
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
            DebugSupport.BUFFERS.log(s"[take] $this -> $lhsAcc & ${MorselDebugSupport.prettyMorselWithHeader("", rhsMorsel).reduce(_ + _)}")
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
  def filterCancelledArguments(accumulator: MorselAccumulator[_], rhsMorsel: MorselExecutionContext): Boolean = {
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

  def close(accumulator: MorselAccumulator[_], rhsMorsel: MorselExecutionContext): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[close] $this -X- $accumulator & ${MorselDebugSupport.prettyMorselWithHeader("", rhsMorsel).reduce(_ + _)}")
    }
    // Check if the argument count is zero -- in the case of the RHS, that means that no more data will ever arrive
    if (rhsArgumentStateMap.hasCompleted(accumulator.argumentRowId)) {
      val rhsAcc = rhsArgumentStateMap.peek(accumulator.argumentRowId)
      // All data from the RHS has arrived
      // If all data is processed and we are the first thread to remove the RHS accumulator (there can be a race here)
      if (rhsAcc != null && !rhsAcc.hasData && rhsArgumentStateMap.remove(accumulator.argumentRowId)) {
        // Clean up the LHS as well
        lhsArgumentStateMap.remove(accumulator.argumentRowId)
      }
    }
    // Decrement for a morsel in the RHS buffer
    forAllArgumentReducers(downstreamArgumentReducers, accumulator.argumentRowIdsForReducers, _.decrement(_))
    tracker.decrement()
  }

  // The LHS does not need any reference counting, because no tasks
  // will ever be produced from the LHS
  object LHSSink extends Sink[IndexedSeq[PerArgument[DATA]]] with AccumulatingBuffer {
    override val argumentSlotOffset: Int = lhsArgumentStateMap.argumentSlotOffset

    override def put(data: IndexedSeq[PerArgument[DATA]]): Unit = {
      if (DebugSupport.BUFFERS.enabled) {
        DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
      }
      var i = 0
      while (i < data.length) {
        lhsArgumentStateMap.update(data(i).argumentRowId, acc => acc.update(data(i).value))
        i += 1
      }
    }

    override def canPut: Boolean = true

    override def initiate(argumentRowId: Long, argumentMorsel: MorselExecutionContext): Unit = {
      if (DebugSupport.BUFFERS.enabled) {
        DebugSupport.BUFFERS.log(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel")
      }
      val argumentRowIdsForReducers: Array[Long] = forAllArgumentReducersAndGetArgumentRowIds(downstreamArgumentReducers, argumentMorsel, (_, _) => Unit)
      lhsArgumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers)
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
  object RHSSink extends Sink[IndexedSeq[PerArgument[MorselExecutionContext]]] with AccumulatingBuffer {
    override val argumentSlotOffset: Int = rhsArgumentStateMap.argumentSlotOffset

    override def put(data: IndexedSeq[PerArgument[MorselExecutionContext]]): Unit = {
      if (DebugSupport.BUFFERS.enabled) {
        DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
      }
      // there is no need to take a lock in this case, because we are sure the argument state is thread safe when needed (is created by state factory)
      var i = 0
      while (i < data.length) {
        val argumentValue = data(i)
        rhsArgumentStateMap.update(argumentValue.argumentRowId, acc => {
          acc.put(argumentValue.value)
          // Increment for a morsel in the RHS buffer
          forAllArgumentReducers(downstreamArgumentReducers, acc.argumentRowIdsForReducers, _.increment(_))
        })
        tracker.increment()
        i += 1
      }
    }

    override def canPut: Boolean = true

    override def initiate(argumentRowId: Long, argumentMorsel: MorselExecutionContext): Unit = {
      if (DebugSupport.BUFFERS.enabled) {
        DebugSupport.BUFFERS.log(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel")
      }
      // Increment for an ArgumentID in RHS's accumulator
      val argumentRowIdsForReducers: Array[Long] = forAllArgumentReducersAndGetArgumentRowIds(downstreamArgumentReducers, argumentMorsel, _.increment(_))
      rhsArgumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers)
      tracker.increment()
    }

    override def increment(argumentRowId: Long): Unit = {
      rhsArgumentStateMap.increment(argumentRowId)
    }

    override def decrement(argumentRowId: Long): Unit = {
      val maybeBuffer = rhsArgumentStateMap.decrement(argumentRowId)
      if (maybeBuffer != null) {
        // Decrement for an ArgumentID in RHS's accumulator
        val argumentRowIdsForReducers = maybeBuffer.argumentRowIdsForReducers
        forAllArgumentReducers(downstreamArgumentReducers, argumentRowIdsForReducers, _.decrement(_))
        tracker.decrement()
      }
    }
  }

}
