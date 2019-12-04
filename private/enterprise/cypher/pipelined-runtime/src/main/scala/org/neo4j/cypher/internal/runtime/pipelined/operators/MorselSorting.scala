/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.Comparator

import org.neo4j.cypher.internal.macros.Require.require
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot}
import org.neo4j.cypher.internal.runtime.pipelined.execution.{Morsel, MorselExecutionContext}
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.values.AnyValue

object MorselSorting {

  def createComparator(orderBy: Seq[ColumnOrder]): Comparator[MorselExecutionContext] =
    orderBy
      .map(MorselSorting.createMorselComparator)
      .reduce((a: Comparator[MorselExecutionContext], b: Comparator[MorselExecutionContext]) => a.thenComparing(b))

  def compareMorselIndexesByColumnOrder(row: MorselExecutionContext)(order: ColumnOrder): Comparator[Integer] = order.slot match {
    case LongSlot(offset, true, _) =>
      (idx1: Integer, idx2: Integer) => {
        row.setCurrentRow(idx1)
        val aVal = row.getLongAt(offset)
        row.setCurrentRow(idx2)
        val bVal = row.getLongAt(offset)
        order.compareNullableLongs(aVal, bVal)
      }

    case LongSlot(offset, false, _) =>
      (idx1: Integer, idx2: Integer) => {
        row.setCurrentRow(idx1)
        val aVal = row.getLongAt(offset)
        row.setCurrentRow(idx2)
        val bVal = row.getLongAt(offset)
        order.compareLongs(aVal, bVal)
      }

    case RefSlot(offset, _, _) =>
      (idx1: Integer, idx2: Integer) => {
        row.setCurrentRow(idx1)
        val aVal = row.getRefAt(offset)
        row.setCurrentRow(idx2)
        val bVal = row.getRefAt(offset)
        order.compareValues(aVal, bVal)
      }
  }

  def createMorselIndexesArray(row: MorselExecutionContext): Array[Integer] = {
    val currentRow = row.getCurrentRow
    val rows = row.getValidRows
    val indexes = new Array[Integer](rows)
    var i = 0
    row.resetToFirstRow()
    while (i < rows) {
      require(row.isValidRow)
      indexes(i) = row.getCurrentRow
      row.moveToNextRow()
      i += 1
    }
    require(!row.isValidRow)
    row.setCurrentRow(currentRow)
    indexes
  }


  /**
    * Sorts the morsel data from array of ordered indices.
    *
    * Does this by sorting into a temp morsel first and then copying back the sorted data.
    */
  def createSortedMorselData(inputRow: MorselExecutionContext, outputToInputIndexes: Array[Integer]): Unit = {
    val numInputRows = inputRow.getValidRows
    // Create a temporary morsel
    // TODO: Do this without creating extra arrays
    val tempMorsel = new Morsel(new Array[Long](numInputRows * inputRow.getLongsPerRow),
                                new Array[AnyValue](numInputRows * inputRow.getRefsPerRow))
    val outputRow = MorselExecutionContext(tempMorsel, inputRow.slots, numInputRows)

    while (outputRow.isValidRow) {
      val fromIndex = outputToInputIndexes(outputRow.getCurrentRow)
      inputRow.setCurrentRow(fromIndex)

      outputRow.copyFrom(inputRow)
      outputRow.moveToNextRow()
    }

    // Copy from output morsel back to input morsel
    inputRow.compactRowsFrom(outputRow)
  }

  def createMorselComparator(order: ColumnOrder): Comparator[MorselExecutionContext] = order.slot match {
    case LongSlot(offset, true, _) =>
      (m1: MorselExecutionContext, m2: MorselExecutionContext) => {
        val aVal = m1.getLongAt(offset)
        val bVal = m2.getLongAt(offset)
        order.compareNullableLongs(aVal, bVal)
      }
    case LongSlot(offset, false, _) =>
      (m1: MorselExecutionContext, m2: MorselExecutionContext) => {
        val aVal = m1.getLongAt(offset)
        val bVal = m2.getLongAt(offset)
        order.compareLongs(aVal, bVal)
      }
    case RefSlot(offset, _, _) =>
      (m1: MorselExecutionContext, m2: MorselExecutionContext) => {
        val aVal = m1.getRefAt(offset)
        val bVal = m2.getRefAt(offset)
        order.compareValues(aVal, bVal)
      }

  }

}
