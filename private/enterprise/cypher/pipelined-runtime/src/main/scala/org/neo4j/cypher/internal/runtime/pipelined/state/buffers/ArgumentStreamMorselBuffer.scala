/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

/**
 * Extension of Morsel buffer that also holds an argument state map in order to track
 * argument rows that do not result in any output rows, i.e. gets filtered out.
 *
 * This buffer sits between two pipelines.
 */
class ArgumentStreamMorselBuffer(id: BufferId,
                                 tracker: QueryCompletionTracker,
                                 downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                                 argumentStateMaps: ArgumentStateMaps,
                                 argumentStateMapId: ArgumentStateMapId
                          )
  extends BaseArgExistsMorselBuffer[MorselData, ArgumentStreamArgumentStateBuffer](id, tracker, downstreamArgumentReducers, argumentStateMaps, argumentStateMapId) {

  override def canPut: Boolean = argumentStateMap.exists(_.canPut)

  override def put(data: IndexedSeq[PerArgument[Morsel]], resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
    }

    var i = 0
    while (i < data.length) {
      argumentStateMap.update(data(i).argumentRowId, {acc =>
        // We increment for each morsel view, otherwise we can reach a count of zero too early, when all the data has arrived in this buffer, but has not been
        // streamed out yet.
        // We have to do the increments in this lambda function, because it can happen that we try to update argument row IDs that are concurrently cancelled and taken
        tracker.increment()
        acc.update(data(i).value, resources)
        forAllArgumentReducers(downstreamArgumentReducers, acc.argumentRowIdsForReducers, _.increment(_))
      })
      i += 1
    }
  }

  override def take(): MorselData = {
    // To achieve streaming behavior, we peek at the data, even if it is not completed yet.
    // To keep input order (i.e., place the null rows at the right position), we give the
    // data out in ascending argument row id order (only in pipelined).
    val argumentState = argumentStateMap.takeOneIfCompletedOrElsePeek()
    val data: MorselData =
      if (argumentState != null) {
        argumentState match {
          case ArgumentStateWithCompleted(completedArgument, true) =>
            if (!completedArgument.didReceiveData) {
              MorselData(IndexedSeq.empty, EndOfEmptyStream, completedArgument.argumentRowIdsForReducers, completedArgument.argumentRow)
            } else {
              val morsels = completedArgument.takeAll()
              if (morsels != null) {
                MorselData(morsels, EndOfNonEmptyStream, completedArgument.argumentRowIdsForReducers, completedArgument.argumentRow)
              } else {
                // We need to return this message to signal that the end of the stream was reached (even if some other Thread got the morsels
                // before us), to close and decrement correctly.
                MorselData(IndexedSeq.empty, EndOfNonEmptyStream, completedArgument.argumentRowIdsForReducers, completedArgument.argumentRow)
              }
            }

          case ArgumentStateWithCompleted(incompleteArgument, false) =>
            val morsels = incompleteArgument.takeAll()
            if (morsels != null) {
              MorselData(morsels, NotTheEnd, incompleteArgument.argumentRowIdsForReducers, incompleteArgument.argumentRow)
            } else {
              // In this case we can simply not return anything, there will arrive more data for this argument row id.
              null.asInstanceOf[MorselData]
            }
        }
      } else {
        null.asInstanceOf[MorselData]
      }
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[take]  $this -> $data")
    }
    data
  }

  override def hasData: Boolean = {
    argumentStateMap.someArgumentStateIsCompletedOr(state => state != null && state.hasData)
  }

  override def clearArgumentState(buffer: ArgumentStreamArgumentStateBuffer): Unit = {
    val morselsOrNull = buffer.takeAll()
    val numberOfDecrements = if (morselsOrNull != null) morselsOrNull.size else 0
    closeOne(EndOfNonEmptyStream, numberOfDecrements, buffer.argumentRowIdsForReducers)
  }

  override def close(data: MorselData): Unit = {
    closeOne(data.argumentStream, data.morsels.size, data.argumentRowIdsForReducers)
  }

  private def closeOne(argumentStream: ArgumentStream, numberOfDecrements: Int, argumentRowIdsForReducers: Array[Long]): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[closeOne] $this -X- $argumentStream , $numberOfDecrements , $argumentRowIdsForReducers")
    }

    argumentStream match {
      case _: EndOfStream =>
        // Decrement that corresponds to the increment in initiate
        tracker.decrement()
        forAllArgumentReducers(downstreamArgumentReducers, argumentRowIdsForReducers, _.decrement(_))
      case _ =>
      // Do nothing
    }

    tracker.decrementBy(numberOfDecrements)

    var i = 0
    while (i < numberOfDecrements) {
      forAllArgumentReducers(downstreamArgumentReducers, argumentRowIdsForReducers, _.decrement(_))
      i += 1
    }
  }

  override def toString: String =
    s"ArgumentStreamMorselBuffer(planId: $argumentStateMapId)$argumentStateMap"
}

// --- Inner Buffer ---

/**
 * For Optional, we need to keep track whether the Buffer held data at any point in time.
 */
trait BufferUsageHistory {
  self: Buffer[_] =>
  /**
   * @return `true` if this buffer held data at any point in time, `false` if it was always empty.
   */
  def didReceiveData: Boolean
}

/**
 * Delegating [[Buffer]] used in argument state maps.
 * Holds data for one argument row id.
 */
class ArgumentStreamArgumentStateBuffer(argumentRowId: Long,
                                        val argumentRow: MorselRow,
                                        inner: Buffer[Morsel] with BufferUsageHistory,
                                        argumentRowIdsForReducers: Array[Long]) extends ArgumentStateBuffer(argumentRowId, inner, argumentRowIdsForReducers) {
  /**
   * @return `true` if this buffer held data at any point in time, `false` if it was always empty.
   */
  def didReceiveData: Boolean = inner.didReceiveData

  /**
   * Take all morsels from the buffer that are currently available.
   */
  def takeAll(): IndexedSeq[Morsel] = {
    var morsel = take()
    if (morsel != null) {
      val morsels = new ArrayBuffer[Morsel]
      do {
        morsels += morsel
        morsel = take()
      } while (morsel != null)
      morsels
    } else {
      null
    }
  }

  override def toString: String = {
    s"${getClass.getSimpleName}(argumentRowId=$argumentRowId, argumentRowIdsForReducers=[${argumentRowIdsForReducers.mkString(",")}], argumentMorsel=$argumentRow, inner=$inner)"
  }
}

object ArgumentStreamArgumentStateBuffer {
  class Factory(stateFactory: StateFactory, operatorId: Id) extends ArgumentStateFactory[ArgumentStateBuffer] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStreamArgumentStateBuffer(argumentRowId, argumentMorsel.snapshot(), new StandardArgumentStreamBuffer[Morsel](stateFactory.newBuffer[Morsel](operatorId)), argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStreamArgumentStateBuffer(argumentRowId, argumentMorsel.snapshot(), new ConcurrentArgumentStreamBuffer[Morsel](stateFactory.newBuffer[Morsel](operatorId)), argumentRowIdsForReducers)
  }
}

class StandardArgumentStreamBuffer[T <: AnyRef](inner: Buffer[T]) extends Buffer[T] with BufferUsageHistory {
  private var _didReceiveData : Boolean = false

  override def put(t: T, resources: QueryResources): Unit = {
    _didReceiveData = true
    inner.put(t, resources)
  }

  def didReceiveData: Boolean = _didReceiveData

  override def foreach(f: T => Unit): Unit = inner.foreach(f)

  override def hasData: Boolean = inner.hasData

  override def take(): T = inner.take()

  override def canPut: Boolean = inner.canPut

  override def toString: String = s"${getClass.getSimpleName}(didReceiveData=${_didReceiveData}, inner=$inner)"
}

class ConcurrentArgumentStreamBuffer[T <: AnyRef](inner: Buffer[T]) extends Buffer[T] with BufferUsageHistory {
  @volatile
  private var _didReceiveData : Boolean = false

  override def put(t: T, resources: QueryResources): Unit = {
    _didReceiveData = true
    inner.put(t, resources)
  }

  def didReceiveData: Boolean = _didReceiveData

  override def foreach(f: T => Unit): Unit = inner.foreach(f)

  override def hasData: Boolean = inner.hasData

  override def take(): T = inner.take()

  override def canPut: Boolean = inner.canPut

  override def toString: String = s"${getClass.getSimpleName}(didReceiveData=${_didReceiveData}, inner=$inner)"
}
