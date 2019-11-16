/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent

import scala.collection.mutable

object FilteringPipelinedExecutionContext {
  def apply(source: MorselExecutionContext) =
    new FilteringPipelinedExecutionContext(source.morsel, source.slots, source.maxNumberOfRows, source.currentRow, source.startRow, source.endRow, source.producingWorkUnitEvent)
}

class FilteringPipelinedExecutionContext(morsel: Morsel,
                                         slots: SlotConfiguration,
                                         maxNumberOfRows: Int,
                                         initialCurrentRow: Int,
                                         initialStartRow: Int,
                                         initialEndRow: Int,
                                         producingWorkUnitEvent: WorkUnitEvent = null,
                                         initialCancelledRows: java.util.BitSet = null)
  extends MorselExecutionContext(morsel, slots, maxNumberOfRows, initialCurrentRow, initialStartRow, initialEndRow, producingWorkUnitEvent) {

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

  def cancelCurrentRow(): Unit = {
    cancelRow(getCurrentRow)
  }

  def cancelRows(cancelledRows: java.util.BitSet): Unit = {
    ensureCancelledRows()
    this.cancelledRows.or(cancelledRows)
  }

  // ARGUMENT COLUMNS

  override def shallowCopy(): FilteringPipelinedExecutionContext =
    new FilteringPipelinedExecutionContext(morsel, slots, maxNumberOfRows, currentRow, startRow, endRow, producingWorkUnitEvent = null, cancelledRows)

  override def moveToNextRow(): Unit = {
    do {
      currentRow += 1
    } while (isCancelled(currentRow))
  }

  @inline
  override def getValidRows: Int = numberOfRows - numberOfCancelledRows // NOTE: This assumes cancelledRows are always cropped by startRow and endRow

  override def getFirstRow: Int = {
    // If this shows up as a hot method we could attempt to cache the result and invalidate it when calling cancelRow() [HN]
    var firstRow = startRow
    while (isCancelled(firstRow) && firstRow < endRow) {
      firstRow += 1
    }
    firstRow
  }

  override def getLastRow: Int = {
    // If this shows up as a hot method we could attempt to cache the result and invalidate it when calling cancelRow() [HN]
    var lastRow = endRow - 1
    while (isCancelled(lastRow) && lastRow > startRow) {
      lastRow -= 1
    }
    lastRow
  }

  override def resetToFirstRow(): Unit = currentRow = getFirstRow
  override def resetToBeforeFirstRow(): Unit = currentRow = getFirstRow - 1
  override def setToAfterLastRow(): Unit = currentRow = getLastRow + 1

  override def isValidRow: Boolean = {
    currentRow >= startRow && currentRow < endRow && !isCancelled(currentRow)
  }

  override def hasNextRow: Boolean = {
    val nextRow = currentRow + 1
    // Since getLastRow could be expensive, we only check it if the next row is cancelled
    nextRow < endRow && (!isCancelled(nextRow) || nextRow < getLastRow)
  }

  /**
   * @param start first index of the view (inclusive start)
   * @param end first index after the view (exclusive end)
   * @return a shallow copy that is configured to only see the configured view.
   */
  override def view(start: Int, end: Int): MorselExecutionContext = {
    val view = shallowCopy()
    view.startRow = start
    view.currentRow = start
    view.endRow = end
    view.cropCancelledRows()
    view
  }

  override def toString: String = {
    s"FilteringMorselExecutionContext[0x${System.identityHashCode(this).toHexString}](longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, maxRows=$maxNumberOfRows, currentRow=$currentRow startRow=$startRow endRow=$endRow $prettyCurrentRow)"
  }

  override def prettyCurrentRow: String =
    if (super.isValidRow) {
      val cancelled = if (isCancelled(currentRow)) "<Cancelled>" else ""
      s"longs: ${morsel.longs.slice(currentRow * longsPerRow, (currentRow + 1) * longsPerRow).mkString("[", ", ", "]")} " +
        s"refs: ${morsel.refs.slice(currentRow * refsPerRow, (currentRow + 1) * refsPerRow).mkString("[", ", ", "]")} $cancelled"
    } else {
      s"<Invalid row>"
    }

  override protected def addPrettyRowMarker(sb: mutable.StringBuilder, row: Int): Unit = {
    if (isCancelled(row)) {
      sb ++= " <Cancelled>"
    }
  }

  //------------------------------------------------------------------------------
  // These methods do not skip over cancelled rows (to be used by the "write cursor" on non-filtered morsels)

  @inline
  final def moveToNextRawRow(): Unit = {
    currentRow += 1
  }

  @inline
  final def moveToRawRow(row: Int): Unit = {
    currentRow = row
  }

  @inline final def resetToFirstRawRow(): Unit = super.resetToFirstRow()
  @inline final def isValidRawRow: Boolean = super.isValidRow

  //------------------------------------------------------------------------------
}
