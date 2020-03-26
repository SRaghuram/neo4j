/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.values.AnyValue

import scala.collection.mutable

object FilteringMorsel {
  final val INSTANCE_SIZE = shallowSizeOfInstance(classOf[FilteringMorsel])
  final val BITSET_INSTANCE_SIZE = shallowSizeOfInstance(classOf[java.util.BitSet])

  def apply(source: Morsel) =
    new FilteringMorsel(source.longs, source.refs, source.slots, source.maxNumberOfRows, source.startRow, source.endRow, source.producingWorkUnitEvent)
}

class FilteringMorsel(longs: Array[Long],
                      refs: Array[AnyValue],
                      slots: SlotConfiguration,
                      maxNumberOfRows: Int,
                      initialStartRow: Int,
                      initialEndRow: Int,
                      producingWorkUnitEvent: WorkUnitEvent = null,
                      initialCancelledRows: java.util.BitSet = null)
  extends Morsel(longs, refs, slots, maxNumberOfRows, initialStartRow, initialEndRow, producingWorkUnitEvent) {

  // ROW CANCELLATION

  private var cancelledRows: java.util.BitSet = _
  if (initialCancelledRows != null) cancelledRows = initialCancelledRows.clone().asInstanceOf[java.util.BitSet]

  private def ensureCancelledRows(): Unit = {
    if (cancelledRows == null) {
      cancelledRows = new java.util.BitSet(maxNumberOfRows)
    }
  }

  // Crops the BitSet to the [startRow, endRow[ range
  def cropCancelledRows(): Unit = {
    if (cancelledRows != null) {
      if (startRow > 0)
        cancelledRows.clear(0, startRow)
      if (endRow < maxNumberOfRows)
        cancelledRows.clear(endRow, maxNumberOfRows)
    }
  }

  def isCancelled(row: Int): Boolean = cancelledRows != null && cancelledRows.get(row)

  def numberOfCancelledRows: Int = if (cancelledRows == null) 0 else cancelledRows.cardinality

  def hasCancelledRows: Boolean = cancelledRows != null && !cancelledRows.isEmpty

  def cancelRow(row: Int): Unit = {
    ensureCancelledRows()
    cancelledRows.set(row)
  }

  def cancelRows(fromIncluding: Int, toExcluded: Int): Unit = {
    ensureCancelledRows()
    this.cancelledRows.set(fromIncluding, toExcluded)
  }

  def cancelRows(cancelledRows: java.util.BitSet): Unit = {
    ensureCancelledRows()
    this.cancelledRows.or(cancelledRows)
  }

  override protected def newCursor(onFirstRow: Boolean): Cursor = {
    val cursor = new FilteringCursor(startRow-1)
    if (onFirstRow) {
      cursor.next()
    }
    cursor
  }

  class FilteringCursor(initialRow: Int) extends Cursor(initialRow) {

    private var nextValidRow: Int = initialRow

    override def setRow(row: Int): Unit = {
      currentRow = row
      nextValidRow = row
    }

    override def setToStart(): Unit = {
      super.setToStart()
      nextValidRow = currentRow
    }

    override def setToEnd(): Unit = {
      super.setToEnd()
      nextValidRow = currentRow
    }

    override def onValidRow(): Boolean = super.onValidRow() && !isCancelled(currentRow)

    override def next(): Boolean = {
      currentRow = findNextValidRow()
      currentRow < endRow
    }

    override def hasNext: Boolean = {
      findNextValidRow()
      nextValidRow < endRow
    }

    private def findNextValidRow(): Int = {
      if (nextValidRow == currentRow) {
        do {
          nextValidRow += 1
        } while (nextValidRow < endRow && isCancelled(nextValidRow))
      }
      nextValidRow
    }
  }

  override def shallowCopy(): FilteringMorsel =
    new FilteringMorsel(longs, refs, slots, maxNumberOfRows, startRow, endRow, producingWorkUnitEvent = null, cancelledRows)

  @inline
  override def numberOfRows: Int = super.numberOfRows - numberOfCancelledRows // NOTE: This assumes cancelledRows are always cropped by startRow and endRow

  override def truncateToRow(row: Int): Unit = {
    endRow = row
    cropCancelledRows()
  }

  /**
   * @param start first index of the view (inclusive start)
   * @param end first index after the view (exclusive end)
   * @return a shallow copy that is configured to only see the configured view.
   */
  override def view(start: Int, end: Int): Morsel = {
    val view = shallowCopy()
    view.startRow = start
    view.endRow = end
    view.cropCancelledRows()
    view
  }

  override def compactRowsFrom(input: Morsel): Unit = {
    super.compactRowsFrom(input)
    if (cancelledRows != null) {
      //we may have to compensate if rows have been filtered out
      endRow = startRow + input.numberOfRows
      cancelledRows = null
    }
  }

  /**
   * Total heap usage of all valid rows (can be a view, so might not be the whole morsel).
   * The reasoning behind this is that the other parts of the morsel would be part of other views in other buffers/argument states and will
   * also be accounted for.
   *
   * NOTE: We do not `override def estimatedHeapUsageOfView: Long` here since even cancelled rows still retain heap memory
   */
  override def estimatedHeapUsage: Long = {
    var usage = FilteringMorsel.INSTANCE_SIZE + estimatedHeapUsageOfView
    if (cancelledRows != null) {
      usage += FilteringMorsel.BITSET_INSTANCE_SIZE +
        HeapEstimator.ARRAY_HEADER_BYTES + // there is an internal array
        cancelledRows.size() >> 3 // size() returns the size in bits of the internal array, so divide by 8 to get size in bytes
    }
    usage
  }

  override def toString: String = {
    s"FilteringMorsel[0x${System.identityHashCode(this).toHexString}](longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, maxRows=$maxNumberOfRows, startRow=$startRow endRow=$endRow)"
  }

  override protected def addPrettyRowMarker(sb: mutable.StringBuilder, row: Int): Unit = {
    if (isCancelled(row)) {
      sb ++= " <Cancelled>"
    }
  }
}
