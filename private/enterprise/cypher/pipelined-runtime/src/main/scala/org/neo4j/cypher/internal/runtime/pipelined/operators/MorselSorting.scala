/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.Comparator

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFactory
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContextOrdering

object MorselSorting {

  def createComparator(orderBy: Seq[ColumnOrder]): Comparator[ReadableRow] =
    SlottedExecutionContextOrdering.asComparator(orderBy)

  def compareMorselIndexesByColumnOrder(row: MorselReadCursor)(order: ColumnOrder): Comparator[Integer] = order.slot match {
    case LongSlot(offset, true, _) =>
      (idx1: Integer, idx2: Integer) => {
        row.setRow(idx1)
        val aVal = row.getLongAt(offset)
        row.setRow(idx2)
        val bVal = row.getLongAt(offset)
        order.compareNullableLongs(aVal, bVal)
      }

    case LongSlot(offset, false, _) =>
      (idx1: Integer, idx2: Integer) => {
        row.setRow(idx1)
        val aVal = row.getLongAt(offset)
        row.setRow(idx2)
        val bVal = row.getLongAt(offset)
        order.compareLongs(aVal, bVal)
      }

    case RefSlot(offset, _, _) =>
      (idx1: Integer, idx2: Integer) => {
        row.setRow(idx1)
        val aVal = row.getRefAt(offset)
        row.setRow(idx2)
        val bVal = row.getRefAt(offset)
        order.compareValues(aVal, bVal)
      }
  }

  def createMorselIndexesArray(morsel: Morsel): Array[Integer] = {
    val rows = morsel.numberOfRows
    val indexes = new Array[Integer](rows)
    var i = 0
    val cursor = morsel.readCursor(onFirstRow = true)
    while (i < rows) {
      checkOnlyWhenAssertionsAreEnabled(cursor.onValidRow())
      indexes(i) = cursor.row
      cursor.next()
      i += 1
    }
    checkOnlyWhenAssertionsAreEnabled(!cursor.onValidRow())
    indexes
  }


  /**
   * Sorts the morsel data from array of ordered indices.
   *
   * Does this by sorting into a temp morsel first and then copying back the sorted data.
   */
  def createSortedMorselData(morsel: Morsel, outputToInputIndexes: Array[Integer]): Unit = {
    val numInputRows = morsel.numberOfRows
    // Create a temporary morsel
    // TODO: Do this without creating extra arrays
    val tempMorsel = MorselFactory.allocate(morsel.slots, numInputRows, morsel.producingWorkUnitEvent)

    val inputCursor = morsel.readCursor()
    val writeCursor = tempMorsel.writeCursor()
    while (writeCursor.next()) {
      val fromIndex = outputToInputIndexes(writeCursor.row)
      inputCursor.setRow(fromIndex)
      writeCursor.copyFrom(inputCursor)
    }
    writeCursor.truncate()

    // Copy from output morsel back to input morsel
    morsel.compactRowsFrom(tempMorsel)
  }

  def createMorselIndexComparator(orderBy: Seq[ColumnOrder], morselCursor: MorselReadCursor): Comparator[Integer] = {
    SlottedExecutionContextOrdering.composeComparator[Integer](MorselSorting.compareMorselIndexesByColumnOrder(morselCursor))(orderBy)
  }
}
