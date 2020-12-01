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
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndPayload

class LHSAccumulatingRHSArgumentStreamingSource[ACC_DATA <: AnyRef,
                                                   LHS_ACC <: MorselAccumulator[ACC_DATA]]
                                        (tracker: QueryCompletionTracker,
                                         downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                                         override val argumentStateMaps: ArgumentStateMaps,
                                         val lhsArgumentStateMapId: ArgumentStateMapId,
                                         val rhsArgumentStateMapId: ArgumentStateMapId,
                                       ) extends JoinBuffer[ACC_DATA, LHS_ACC, MorselData] {

  private val lhsArgumentStateMap = argumentStateMaps(lhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[LHS_ACC]]
  private val rhsArgumentStateMap = argumentStateMaps(rhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[ArgumentStreamArgumentStateBuffer]]

  checkOnlyWhenAssertionsAreEnabled {
    tracker.addCompletionAssertion { () =>
      if (lhsArgumentStateMap.exists(_ => true) || rhsArgumentStateMap.exists(_ => true)) {
        throw new RuntimeResourceLeakException("Argument state maps should be empty.")
      }
    }
  }

  override def hasData: Boolean = {
    lhsArgumentStateMap.peekCompleted().exists(lhsAcc => {
      rhsArgumentStateMap.hasCompleted(lhsAcc.argumentRowId) || {
        val rhsArgumentState = rhsArgumentStateMap.peek(lhsAcc.argumentRowId)
        rhsArgumentState != null && rhsArgumentState.hasData
      }
    })
  }

  override def take(): AccumulatorAndPayload[ACC_DATA, LHS_ACC, MorselData] = {
    val lhsAccs = lhsArgumentStateMap.peekCompleted()
    while (lhsAccs.hasNext) {
      val lhsAcc = lhsAccs.next()
      val rhsMorselData = tryTakeRhs(lhsAcc)
      if (rhsMorselData != null) {
        return AccumulatorAndPayload[ACC_DATA, LHS_ACC, MorselData](lhsAcc, rhsMorselData)
      }
    }
    null
  }

  private def tryTakeRhs(lhsAcc: LHS_ACC): MorselData = {
    val rhsBuffer = rhsArgumentStateMap.takeIfCompletedOrElsePeek(lhsAcc.argumentRowId)
    if (rhsBuffer != null) {
      rhsBuffer match {
        case ArgumentStateWithCompleted(completedArgumentState, true) =>
          if (!completedArgumentState.didReceiveData) {
            MorselData(IndexedSeq.empty, EndOfEmptyStream, completedArgumentState.argumentRowIdsForReducers, completedArgumentState.argumentRow)
          } else {
            val morsels = completedArgumentState.takeAll()
            if (morsels != null) {
              MorselData(morsels, EndOfNonEmptyStream, completedArgumentState.argumentRowIdsForReducers, completedArgumentState.argumentRow)
            } else {
              // We need to return this message to signal that the end of the stream was reached (even if some other Thread got the morsels
              // before us), to close and decrement correctly.
              MorselData(IndexedSeq.empty, EndOfNonEmptyStream, completedArgumentState.argumentRowIdsForReducers, completedArgumentState.argumentRow)
            }
          }

        case ArgumentStateWithCompleted(incompleteArgumentState, false) =>
          val morsels = incompleteArgumentState.takeAll()
          if (morsels != null) {
            MorselData(morsels, NotTheEnd, incompleteArgumentState.argumentRowIdsForReducers, incompleteArgumentState.argumentRow)
          } else {
            // In this case we can simply not return anything, there will arrive more data for this argument row id.
            null.asInstanceOf[MorselData]
          }
      }
    } else {
      null.asInstanceOf[MorselData]
    }
  }

  /**
   * Remove all rows related to cancelled argumentRowIds from `morsel`.
   * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
   *
   * @param rhsMorsel   the input morsel from the RHS
   * @param accumulator the accumulator
   * @return `true` iff both the morsel and the accumulator are cancelled
   */
  override def filterCancelledArguments(accumulator: MorselAccumulator[_], rhsMorsel: Morsel): Boolean = {
    // TODO
    false
  }

  override def clearAll(): Unit = {
    lhsArgumentStateMap.clearAll((buffer: LHS_ACC) => {
      if (!rhsArgumentStateMap.peek(buffer.argumentRowId).hasData) {
        // If the RHS is empty, we need to decrement away the extra increment that came from LHSSink.decrement()
        // Count Type: Empty RHS
        tracker.decrement()
      }
    })
    rhsArgumentStateMap.clearAll(buffer => {
      var morsel = buffer.take()
      while (morsel != null) {
        forAllArgumentReducers(downstreamArgumentReducers, buffer.argumentRowIdsForReducers, _.decrement(_))
        // Count Type: RHS Put per Morsel
        tracker.decrement()
        morsel = buffer.take()
      }
    })
  }

  override def toString: String = s"${getClass.getSimpleName}(lhs:$lhsArgumentStateMapId, rhs:$rhsArgumentStateMapId)"

  override def close(accumulator: MorselAccumulator[_], morselData: MorselData): Unit = {
    val argumentRowIdsForReducers = accumulator.argumentRowIdsForReducers
    val argumentRowId = accumulator.argumentRowId

    val nbrOfMorsels = morselData.morsels.size
    val nbrOfTrackerDecrements = morselData.argumentStream match {
      case _: EndOfStream =>
        checkOnlyWhenAssertionsAreEnabled(rhsArgumentStateMap.peek(argumentRowId) == null, "RHS accumulator should have already been removed in take()")
        // No more data will ever arrive for this argument
        val lhsAccumulator = lhsArgumentStateMap.remove(argumentRowId)
        lhsAccumulator.close()
        // We still need to decrement away the extra increment that came from LHSSink.decrement()
        nbrOfMorsels /*Count Type: RHS Put per Morsel*/ + 1 /*Count Type: Empty RHS*/
      case _ =>
        // Count Type: RHS Put per Morsel
        nbrOfMorsels
    }

    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[close] $this -X- ${morselData.argumentStream} , $nbrOfTrackerDecrements , $argumentRowIdsForReducers")
    }

    tracker.decrementBy(nbrOfTrackerDecrements)

    forAllArgumentReducers(downstreamArgumentReducers, argumentRowIdsForReducers,
      (buffer, id) => {
        var i = 0
        while (i < nbrOfMorsels) {
          buffer.decrement(id)
          i += 1
        }
      })
  }
}

// The LHS does not need any reference counting, because no tasks
// will ever be produced from the LHS
class LeftOuterLhsAccumulatingSink[DATA <: AnyRef, LHS_ACC <: MorselAccumulator[DATA]](val lhsArgumentStateMapId: ArgumentStateMapId,
                                                                                       downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                                                                                       override val argumentStateMaps: ArgumentStateMaps,
                                                                                       tracker: QueryCompletionTracker)
  extends ArgumentCountUpdater
  with Sink[IndexedSeq[PerArgument[DATA]]]
  with AccumulatingBuffer {

  private val lhsArgumentStateMap = argumentStateMaps(lhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[LHS_ACC]]

  override val argumentSlotOffset: Int = lhsArgumentStateMap.argumentSlotOffset

  override def toString: String = s"${getClass.getSimpleName}($lhsArgumentStateMap)"

  override def put(data: IndexedSeq[PerArgument[DATA]], resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
    }
    var i = 0
    while (i < data.length) {
      lhsArgumentStateMap.update(data(i).argumentRowId, acc => acc.update(data(i).value, resources))
      i += 1
    }
  }

  override def canPut: Boolean = !lhsArgumentStateMap.hasCompleted

  override def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[init]  $this <- argumentRowId=$argumentRowId from $argumentMorsel with initial count $initialCount")
    }
    val argumentRowIdsForReducers: Array[Long] = forAllArgumentReducersAndGetArgumentRowIds(downstreamArgumentReducers, argumentMorsel, (_, _) => Unit)
    lhsArgumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers, initialCount)
  }

  override def increment(argumentRowId: Long): Unit = {
    lhsArgumentStateMap.increment(argumentRowId)
  }

  override def decrement(argumentRowId: Long): Unit = {
    if (lhsArgumentStateMap.decrement(argumentRowId) != null) {
      // Increment tracker here, we need this for the empty RHS case
      // Count Type: Empty RHS
      tracker.increment()
    }
  }
}

// We need to reference count both tasks and argument IDs on the RHS.
// Tasks need to be tracked since the RHS accumulator's Buffer is used multiple times
// to spawn tasks, unlike in the MorselArgumentStateBuffer where you only take the accumulator once.
class LeftOuterRhsStreamingSink(val rhsArgumentStateMapId: ArgumentStateMapId,
                                downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                                override val argumentStateMaps: ArgumentStateMaps,
                                tracker: QueryCompletionTracker) extends ArgumentCountUpdater
                                                        with Sink[IndexedSeq[PerArgument[Morsel]]]
                                                        with AccumulatingBuffer {
  private val rhsArgumentStateMap = argumentStateMaps(rhsArgumentStateMapId).asInstanceOf[ArgumentStateMap[ArgumentStreamArgumentStateBuffer]]

  override val argumentSlotOffset: Int = rhsArgumentStateMap.argumentSlotOffset

  override def toString: String = s"${getClass.getSimpleName}($rhsArgumentStateMap)"

  override def put(data: IndexedSeq[PerArgument[Morsel]], resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
    }
    // there is no need to take a lock in this case, because we are sure the argument state is thread safe when needed (is created by state factory)
    var i = 0
    while (i < data.length) {
      val argumentValue = data(i)
      rhsArgumentStateMap.update(argumentValue.argumentRowId, acc => {
        acc.put(argumentValue.value, resources)
        // Increment for a morsel in the RHS buffer
        forAllArgumentReducers(downstreamArgumentReducers, acc.argumentRowIdsForReducers, _.increment(_))
      })
      // Count Type: RHS Put per Morsel
      // Increment tracker by 1 for each buffered "argument morsel"
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
    rhsArgumentStateMap.initiate(argumentRowId, argumentMorsel, argumentRowIdsForReducers, initialCount)
    // Count Type: RHS Accumulator Init
    tracker.increment()
  }

  override def increment(argumentRowId: Long): Unit = {
    rhsArgumentStateMap.increment(argumentRowId)
  }


  override def decrement(argumentRowId: Long): Unit = {
    val completedBuffer = rhsArgumentStateMap.decrement(argumentRowId)
    if (completedBuffer != null) {

      // Decrement for an ArgumentID in RHS's accumulator
      val argumentRowIdsForReducers = completedBuffer.argumentRowIdsForReducers
      forAllArgumentReducers(downstreamArgumentReducers, argumentRowIdsForReducers, _.decrement(_))
      // Count Type: RHS Accumulator Init
      tracker.decrement()
    }
  }
}
