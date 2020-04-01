/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import java.util

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentCountUpdater
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder
import org.neo4j.cypher.internal.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

/**
 * Extension of Morsel buffer that also holds an argument state map in order to track
 * argument rows that do not result in any output rows, i.e. gets filtered out.
 *
 * This buffer sits between two pipelines.
 */
abstract class BaseArgExistsMorselBuffer[PRODUCES <: AnyRef](id: BufferId,
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

  protected val argumentStateMap: ArgumentStateMap[OptionalArgumentStateBuffer] =
    argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[OptionalArgumentStateBuffer]]

  override val argumentSlotOffset: Int = argumentStateMap.argumentSlotOffset

  override def canPut: Boolean = argumentStateMap.exists(_.canPut)

  override def put(data: IndexedSeq[PerArgument[Morsel]]): Unit = {
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
        acc.update(data(i).value)
        forAllArgumentReducers(downstreamArgumentReducers, acc.argumentRowIdsForReducers, _.increment(_))
      })
      i += 1
    }
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
    argumentStateMap.clearAll(buffer => {
      val morselsOrNull = buffer.takeAll()
      val numberOfDecrements = if (morselsOrNull != null) morselsOrNull.size else 0
      closeOne(EndOfNonEmptyStream, numberOfDecrements, buffer.argumentRowIdsForReducers)
    })
  }

  protected def closeOne(argumentStream: ArgumentStream, numberOfDecrements: Int, argumentRowIdsForReducers: Array[Long]): Unit = {
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

  def filterCancelledArguments(accumulator: MorselAccumulator[_]): Boolean = {
    false
  }

  override def toString: String =
    s"BaseArgExistsMorselBuffer(planId: $argumentStateMapId)$argumentStateMap"
}

// --- Messaging protocol ---

/**
 * Some Morsels for one argument row id. Depending on the [[ArgumentStream]] there might be more data for this argument row id.
 */
case class MorselData(morsels: IndexedSeq[Morsel],
                      argumentStream: ArgumentStream,
                      argumentRowIdsForReducers: Array[Long])

trait ArgumentStream
trait EndOfStream extends ArgumentStream

/**
 * The end of data for one argument row id, when there was actually no data (i.e. everything was filtered out).
 * @param viewOfArgumentRow the argument row for the id, as obtained from the [[MorselApplyBuffer]]
 */
case class EndOfEmptyStream(viewOfArgumentRow: MorselRow) extends EndOfStream

/**
 * The end of data for one argument row id, when there was data.
 */
case object EndOfNonEmptyStream extends EndOfStream

/**
 * There will be more data for this argument row id.
 */
case object NotTheEnd extends ArgumentStream

// --- Inner Buffer ---

/**
 * For Optional, we need to keep track whether the Buffer held data at any point in time.
 */
trait OptionalBuffer {
  self: Buffer[_] =>
  /**
   * @return `true` if this buffer held data at any point in time, `false` if it was always empty.
   */
  def didReceiveData: Boolean
}

/**
 * Delegating [[Buffer]] used in argument state maps.
 * Holds data for one argument row id in an [[OptionalBuffer]].
 */
class OptionalArgumentStateBuffer(argumentRowId: Long,
                                  val argumentRow: MorselRow,
                                  inner: Buffer[Morsel] with OptionalBuffer,
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
    s"OptionalArgumentStateBuffer(argumentRowId=$argumentRowId, argumentRowIdsForReducers=[${argumentRowIdsForReducers.mkString(",")}], argumentMorsel=$argumentRow, inner=$inner)"
  }
}

object OptionalArgumentStateBuffer {
  class Factory(stateFactory: StateFactory, operatorId: Id) extends ArgumentStateFactory[ArgumentStateBuffer] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new OptionalArgumentStateBuffer(argumentRowId, argumentMorsel.snapshot(), new StandardOptionalBuffer[Morsel](stateFactory.newBuffer[Morsel](operatorId)), argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new OptionalArgumentStateBuffer(argumentRowId, argumentMorsel.snapshot(), new ConcurrentOptionalBuffer[Morsel](stateFactory.newBuffer[Morsel](operatorId)), argumentRowIdsForReducers)
  }
}

class StandardOptionalBuffer[T <: AnyRef](inner: Buffer[T]) extends Buffer[T] with OptionalBuffer {
  private var _didReceiveData : Boolean = false

  override def put(t: T): Unit = {
    _didReceiveData = true
    inner.put(t)
  }

  def didReceiveData: Boolean = _didReceiveData

  override def foreach(f: T => Unit): Unit = inner.foreach(f)

  override def hasData: Boolean = inner.hasData

  override def take(): T = inner.take()

  override def canPut: Boolean = inner.canPut

  override def iterator: util.Iterator[T] = inner.iterator
}

class ConcurrentOptionalBuffer[T <: AnyRef](inner: Buffer[T]) extends Buffer[T] with OptionalBuffer {
  @volatile
  private var _didReceiveData : Boolean = false

  override def put(t: T): Unit = {
    _didReceiveData = true
    inner.put(t)
  }

  def didReceiveData: Boolean = _didReceiveData

  override def foreach(f: T => Unit): Unit = inner.foreach(f)

  override def hasData: Boolean = inner.hasData

  override def take(): T = inner.take()

  override def canPut: Boolean = inner.canPut

  override def iterator: util.Iterator[T] = inner.iterator

  override def toString: String = s"OptionalMorselBuffer(didReceiveData=${_didReceiveData}, inner=${inner.toString})"
}
