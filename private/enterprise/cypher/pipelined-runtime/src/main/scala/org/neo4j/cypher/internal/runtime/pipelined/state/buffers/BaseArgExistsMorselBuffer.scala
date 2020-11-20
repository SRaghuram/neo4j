/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
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
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardAggregators.SHALLOW_SIZE
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.annotation.tailrec

/**
 * Extension of Morsel buffer that also holds an argument state map in order to track
 * argument rows that do not result in any output rows, i.e. gets filtered out.
 *
 * This buffer sits between two pipelines.
 */
abstract class BaseArgExistsMorselBuffer[PRODUCES <: AnyRef, S <: ArgumentState](
                                                             id: BufferId,
                                                             tracker: QueryCompletionTracker,
                                                             downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
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
    s"BaseArgExistsMorselBuffer($argumentStateMap)"
}

// --- Messaging protocol ---

/**
 * Some Morsels for one argument row id. Depending on the [[ArgumentStream]] there might be more data for this argument row id.
 * @param viewOfArgumentRow the argument row for the id, as obtained from the [[MorselApplyBuffer]]
 */
case class MorselData(morsels: IndexedSeq[Morsel],
                      argumentStream: ArgumentStream,
                      argumentRowIdsForReducers: Array[Long],
                      viewOfArgumentRow: MorselRow) {

  /**
   * Returns a [[MorselReadCursor]] over all containing morsels.
   */
  def readCursor(onFirstRow: Boolean = false): MorselReadCursor = {
    if (morsels.nonEmpty) new Cursor(onFirstRow)
    else Morsel.empty.readCursor(onFirstRow)
  }

  class Cursor(onFirstRow: Boolean) extends MorselReadCursor {
    require(MorselData.this.morsels.nonEmpty)

    private var morselCursor: MorselReadCursor = MorselData.this.morsels(0).readCursor(onFirstRow = false)
    private var nextMorselDataIndex = 1

    if (onFirstRow) next()

    @inline
    private[this] def setMorselCursor(index: Int) = {
      morselCursor = MorselData.this.morsels(index).readCursor()
      nextMorselDataIndex = index + 1
    }

    override def getLongAt(offset: Int): Long = morselCursor.getLongAt(offset)
    override def getRefAt(offset: Int): AnyValue = morselCursor.getRefAt(offset)
    override def getByName(name: String): AnyValue = morselCursor.getByName(name)
    override def getLinenumber: Option[ResourceLinenumber] = morselCursor.getLinenumber
    override def setRow(row: Int): Unit = morselCursor.setRow(row)

    override def setToStart(): Unit = setMorselCursor(0)

    override def setToEnd(): Unit = {
      setMorselCursor(MorselData.this.morsels.size - 1)
      morselCursor.setToEnd()
    }

    override def onValidRow(): Boolean = morselCursor != null && morselCursor.onValidRow()

    @tailrec
    override final def next(): Boolean = {
      if (morselCursor.next()) {
        true
      } else if (nextMorselDataIndex >= MorselData.this.morsels.size) {
        false
      } else {
        morselCursor = MorselData.this.morsels(nextMorselDataIndex).readCursor()
        nextMorselDataIndex += 1
        next()
      }
    }

    override def hasNext: Boolean = morselCursor.hasNext || upcomingMorselsHasNext

    private def upcomingMorselsHasNext(): Boolean = {
      var i = nextMorselDataIndex
      val size = MorselData.this.morsels.size
      while (i < size) {
        if (MorselData.this.morsels(i).hasData) return true
        i += 1
      }
      false
    }

    override def snapshot(): MorselRow = morselCursor.snapshot()

    override def row: Int = throw new UnsupportedOperationException("row not supported in MorselData.Cursor")

    override def morsel: Morsel = morselCursor.morsel

    override def longOffset(offsetInRow: Int): Int = morselCursor.longOffset(offsetInRow)

    override def refOffset(offsetInRow: Int): Int = morselCursor.refOffset(offsetInRow)

    override def shallowInstanceHeapUsage: Long = SHALLOW_SIZE

    override def getCachedProperty(key: ASTCachedProperty): Value = morselCursor.getCachedProperty(key)

    override def getCachedPropertyAt(offset: Int): Value = morselCursor.getCachedPropertyAt(offset)

    override def setCachedProperty(key: ASTCachedProperty, value: Value): Unit = morselCursor.setCachedProperty(key, value)

    override def setCachedPropertyAt(offset: Int, value: Value): Unit = morselCursor.setCachedPropertyAt(offset, value)

    override def copyAllToSlottedRow(target: SlottedRow): Unit = morselCursor.copyAllToSlottedRow(target)

    override def copyToSlottedRow(target: SlottedRow, nLongs: Int, nRefs: Int): Unit = morselCursor.copyToSlottedRow(target, nLongs, nRefs)
  }

  object Cursor {
    final val SHALLOW_SIZE = shallowSizeOfInstance(classOf[Cursor])
  }
}

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
