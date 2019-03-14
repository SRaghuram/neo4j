/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attribute, Id}

/**
  * Accumulator of morsels. Has internal state which it updates using provided morsel.
  */
trait MorselAccumulator {

  /**
    * Update internal state using the provided morsel.
    */
  def update(morsel: MorselExecutionContext): Unit

  /**
    * The ID of the argument row for this accumulator.
    */
  def argumentRowId: Long
}

trait MorselAccumulatorFactory[ACC <: MorselAccumulator] {
  def newAccumulator(argumentRowId: Long): ACC
}

/**
  * Maps every argument to one `MorselAccumulator`/`ACC`.
  */
trait ArgumentStateMap[ACC <: MorselAccumulator] {

  /**
    * Update the MorselAccumulator related to `argument` and decrement
    * the argument counter. Also remove the [[owningPlanId]] counter
    * from `morsel`.
    */
  def update(morsel: MorselExecutionContext): Unit

  def filter[U](morsel: MorselExecutionContext, onArgument: (ACC, Long) => U, onRow: (U, MorselExecutionContext) => Boolean): Unit

  /**
    * Take the MorselAccumulators of all complete arguments. The MorselAccumulators will
    * be removed from the ArgumentStateMap and cannot be taken again or modified after this call.
    */
  def takeCompleted(): Iterable[ACC]

  /**
    * Returns `true` iff there is a completed argument.
    * @return
    */
  def hasCompleted: Boolean

  /**
    * Initiate state and counting for a new argument.
    */
  def initiate(argument: Long): Unit

  /**
    * Increment the argument counter for `argument`.
    */
  def increment(argument: Long): Unit

  /**
    * Decrement the argument counter for `argument`.
    */
  def decrement(argument: Long): Unit

  /**
    * Plan which owns this argument state map.
    */
  def owningPlanId: Id

  /**
    * Slot offset of the argument slot.
    */
  def argumentSlotOffset: Int
}

class ArgumentStateMaps() extends Attribute[ArgumentStateMap[_ <: MorselAccumulator]]

object ArgumentStateMap {

  /**
    * For each argument row id at `argumentSlotOffset`, create a view over `morsel`
    * displaying only the rows derived from this argument rows, and execute `f` on this view.
    */
  def foreachArgument(argumentSlotOffset: Int,
                      morsel: MorselExecutionContext,
                      f: (Long, MorselExecutionContext) => Unit): Unit = {

    while (morsel.isValidRow) {
      val arg = morsel.getLongAt(argumentSlotOffset)
      val start: Int = morsel.getCurrentRow
      while (morsel.isValidRow && morsel.getLongAt(argumentSlotOffset) == arg) {
        morsel.moveToNextRow()
      }
      val end: Int = morsel.getCurrentRow
      val view = morsel.view(start, end)
      f(arg, view)
    }
  }

  /**
    * For each argument row id at `argumentSlotOffset`, create a filter state (`FILTER_STATE`). This
    * filter state is then used to call `onRow` on each row with the argument row id. If `onRow`
    * returns true, the row is retained, otherwise it's discarded.
    *
    * @param morsel the morsel to filter
    * @param onArgument onArgument(argumentRowId, nRows): FilterState
    * @param onRow onRow(FilterState, morselRow): Boolean
    * @tparam FILTER_STATE state used for filtering
    */
  def filter[FILTER_STATE](argumentSlotOffset: Int,
                morsel: MorselExecutionContext,
                onArgument: (Long, Long) => FILTER_STATE,
                onRow: (FILTER_STATE, MorselExecutionContext) => Boolean): Unit = {

    val readingRow = morsel
    val writingRow = readingRow.shallowCopy()

    while (readingRow.isValidRow) {
      val arg = readingRow.getLongAt(argumentSlotOffset)
      val start: Int = readingRow.getCurrentRow
      while (readingRow.isValidRow && readingRow.getLongAt(argumentSlotOffset) == arg) {
        readingRow.moveToNextRow()
      }
      val end: Int = readingRow.getCurrentRow
      readingRow.moveToRow(start)

      val filterState = onArgument(arg, end - start)
      while (readingRow.getCurrentRow < end) {
        if (onRow(filterState, readingRow)) {
          writingRow.copyFrom(readingRow)
          writingRow.moveToNextRow()
        }
        readingRow.moveToNextRow()
      }
    }

    readingRow.finishedWritingUsing(writingRow)
  }
}
