/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.ArgumentState

import scala.collection.mutable.ArrayBuffer

/**
  * Maps every argument to one `ArgumentState`/`S`.
  */
trait ArgumentStateMap[S <: ArgumentState] {

  /**
    * Update the [[ArgumentState]] related to `argument` and decrement
    * the argument counter.
    */
  def update(morsel: MorselExecutionContext, onState: (S, MorselExecutionContext) => Unit): Unit

  /**
    * Filter the input morsel using the [[ArgumentState]] related to `argument`.
    *
    * @param onArgument is called once per argumentRowId, to generate a filter state.
    * @param onRow      is called once per row, and given the filter state of the current argumentRowId
    */
  def filter[FILTER_STATE](morsel: MorselExecutionContext,
                           onArgument: (S, Long) => FILTER_STATE,
                           onRow: (FILTER_STATE, MorselExecutionContext) => Boolean): Unit

  /**
    * Filter away cancelled argument rows using the [[ArgumentState]] related to `argument`.
    *
    * @param isCancelled is called once per argumentRowId.
    *                    If true, rows for the argumentRowId will be discarded, otherwise they will be retained.
    */
  def filterCancelledArguments(morsel: MorselExecutionContext,
                               isCancelled: S => Boolean): IndexedSeq[Long]

  /**
    * Take the [[ArgumentState]] of one complete arguments. The [[ArgumentState]] will
    * be removed from the [[ArgumentStateMap]] and cannot be taken again or modified after this call.
    */
  def takeOneCompleted(): S

  /**
    * Returns the [[ArgumentState]] of each completed argument, but does not remove them from the [[ArgumentStateMap]].
    */
  def peekCompleted(): Iterator[S]

  /**
    * Returns the [[ArgumentState]] for the specified argumentId, but does not remove it from the [[ArgumentStateMap]].
    * @return the [[ArgumentState]] for the provided argumentId, or null if none exists
    */
  def peek(argumentId: Long): S

  /**
    * Returns `true` iff there is a completed argument.
    * @return
    */
  def hasCompleted: Boolean

  /**
    * Returns `true` iff the argument is completed.
    */
  def hasCompleted(argument: Long): Boolean

  /**
    * Removes the state of this argument.
    * @return `true` if the argument was removed, `false` if it had been removed earlier.
    **/
  def remove(argument: Long): Boolean

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
    * @return true iff count has reached zero
    */
  def decrement(argument: Long): Boolean

  /**
    * ID of this ArgumentStateMap
    */
  def argumentStateMapId: ArgumentStateMapId

  /**
    * Slot offset of the argument slot.
    */
  def argumentSlotOffset: Int
}

/**
  * Static methods and interfaces connected to ArgumentStateMap
  */
object ArgumentStateMap {

  /**
    * State that belongs to one argumentRowId and is kept in the [[ArgumentStateMap]]
    */
  trait ArgumentState {
    /**
      * The ID of the argument row for this state.
      */
    def argumentRowId: Long
  }

  /**
    * A state that keeps track whether work related to the argument id is cancelled.
    */
  trait WorkCanceller extends ArgumentState {
    def isCancelled: Boolean
  }

  /**
    * Accumulator of morsels. Has internal state which it updates using provided morsel.
    */
  trait MorselAccumulator extends ArgumentState {

    /**
      * Update internal state using the provided morsel.
      */
    def update(morsel: MorselExecutionContext): Unit
  }

  /**
    * Operators that use ArgumentStateMaps need to implement this to be able to instantiate
    * the corresponding ArgumentStateMap.
    */
  trait ArgumentStateFactory[S <: ArgumentState] {
    def newStandardArgumentState(argumentRowId: Long): S
    def newConcurrentArgumentState(argumentRowId: Long): S
  }

  /**
    * Mapping of IDs to the corresponding ArgumentStateMap.
    */
  trait ArgumentStateMaps {
    /**
      * Get the argument state map for the given id.
      */
    def apply(argumentStateMapId: ArgumentStateMapId): ArgumentStateMap[_ <: ArgumentState]
  }

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

    val readingRow = morsel.shallowCopy()
    readingRow.resetToFirstRow()

    val writingRow = readingRow.shallowCopy()
    var newCurrentRow = -1

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
        if (readingRow.getCurrentRow == morsel.getCurrentRow)
          newCurrentRow = writingRow.getCurrentRow

        if (onRow(filterState, readingRow)) {
          writingRow.copyFrom(readingRow)
          writingRow.moveToNextRow()
        }
        readingRow.moveToNextRow()
      }
    }

    morsel.moveToRow(newCurrentRow)
    morsel.finishedWritingUsing(writingRow)
  }

  def filterCancelledArguments(argumentSlotOffset: Int,
                               morsel: MorselExecutionContext,
                               isCancelledCheck: Long => Boolean): IndexedSeq[Long] = {

    val readingRow = morsel.shallowCopy()
    readingRow.resetToFirstRow()
    val writingRow = readingRow.shallowCopy()
    val cancelled = new ArrayBuffer[Long]
    var newCurrentRow = -1

    while (readingRow.isValidRow) {
      val arg = readingRow.getLongAt(argumentSlotOffset)
      val isCancelled = isCancelledCheck(arg)
      if (isCancelled)
        cancelled += arg

      while (readingRow.isValidRow && readingRow.getLongAt(argumentSlotOffset) == arg) {
        if (readingRow.getCurrentRow == morsel.getCurrentRow)
          newCurrentRow = writingRow.getCurrentRow

        if (!isCancelled) {
          writingRow.copyFrom(readingRow)
          writingRow.moveToNextRow()
        }
        readingRow.moveToNextRow()
      }
    }

    morsel.moveToRow(newCurrentRow)
    morsel.finishedWritingUsing(writingRow)
    cancelled
  }
}
