/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.slotted.SlottedCompatible
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.Measurable

/**
  * Row in a morsel.
  */
trait MorselDerivedRow extends SlottedCompatible with Measurable {
  /**
    * Offset of this row in the morsel.
    */
  def row: Int

  /**
    * Morsel holding this row.
    */
  def morsel: Morsel

  /**
    * Computes for a specific slot in this row, the offset in the morsel long array.
    */
  def longOffset(offsetInRow: Int): Int

  /**
    * Computes for a specific slot in this row, the offset in the morsel ref array.
    */
  def refOffset(offsetInRow: Int): Int

  /**
   * Heap usage of long and ref arrays for the current row.
   */
  def estimatedLongsAndRefsHeapUsage: Long = {
    var usage = morsel.longsPerRow * java.lang.Long.BYTES.toLong  + morsel.refsPerRow * HeapEstimator.OBJECT_REFERENCE_BYTES
    var i = 0
    while (i < morsel.refsPerRow) {
      val ref = morsel.refs(refOffset(i))
      if (ref != null) {
        usage += ref.estimatedHeapUsage()
      }
      i += 1
    }
    usage
  }

  /**
   * Heap usage of the current row object instance.
   */
  def shallowInstanceHeapUsage: Long

  override def estimatedHeapUsage(): Long = estimatedLongsAndRefsHeapUsage + shallowInstanceHeapUsage
}

/**
  * Cursor used to select and iterate over the rows in a morsel.
  */
trait MorselCursor extends MorselDerivedRow {

  /**
    * Point this cursor to a provided row. The row is interpreted as the row offset
    * into the morsel. This operation might leave the cursor pointing to an out-of-bounds
    * or cancelled row.
    */
  def setRow(row: Int): Unit

  /**
    * Point this cursor to the position before the first valid row.
    */
  def setToStart(): Unit

  /**
    * Point this cursor to the position after the last valid row.
    */
  def setToEnd(): Unit

  /**
    * Return true if the cursor point to an in-bounds and not cancelled row.
    */
  def onValidRow(): Boolean

  /**
    * Advance the cursor to the next valid row, or to the end if there are no more rows.
    * @return true if the cursor now points to a valid row, otherwise false.
    */
  def next(): Boolean

  /**
    * @return true if there is another valid row, otherwise false.
    */
  def hasNext: Boolean

  /**
    * Create an immutable snapshot of the current row.
    */
  def snapshot(): MorselRow
}

// We need the Morsel*Cursors to be actual traits instead of type aliases in the
// package object, because otherwise operator fusion becomes super-confused.
/**
  * MorselCursor used to read morsel data.
  */
trait MorselReadCursor extends MorselCursor with ReadableRow

/**
  * MorselCursor used to write data to a morsel.
  */
trait MorselWriteCursor extends MorselCursor with WritableRow {

  /**
    * Invalidate the current and any later rows in the morsel.
    */
  def truncate(): Unit

  /**
    * Copy all data from the provided row into the cursor row.
    */
  def copyFrom(from: MorselDerivedRow): Unit

  /**
    * Copy all data from the provided row into the cursor row.
    */
  def copyFromSlottedRowOrCursor(from: ReadableRow): Unit
}

/**
  * MorselCursor used to read and write data to a morsel, and for integration with
  * interpreted and slotted.
  */
trait MorselFullCursor extends MorselReadCursor with MorselWriteCursor with CypherRow

object ArgumentSlots {

  def getArgumentAt(row: ReadableRow, offset: Int): Long =
    if (offset == TopLevelArgument.SLOT_OFFSET) 0L
    else row.getLongAt(offset)

  def setArgumentAt(row: WritableRow, offset: Int, argument: Long): Unit =
    if (offset == TopLevelArgument.SLOT_OFFSET) {
      TopLevelArgument.assertTopLevelArgument(argument)
    } else {
      row.setLongAt(offset, argument)
    }
}
