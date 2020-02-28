/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.FilteringMorsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted

import scala.collection.mutable.ArrayBuffer

/**
 * Maps every argument to one `ArgumentState`/`S`.
 */
trait ArgumentStateMap[S <: ArgumentState] {

  def clearAll(f: S => Unit): Unit

  /**
   * Update the state for the given argument row id.
   * Make sure that the controller exists and the state is not taken, and only apply the function then.
   *
   * @param argumentRowId the argument row id.
   * @param onState the function to apply on the state.
   */
  def update(argumentRowId: Long, onState: S => Unit): Unit

  /**
    * Skip some number of items of the input morsel using the [[ArgumentState]] related to `argument`.
    * @param morsel the morsel in question
    * @param reserve called to get a number of items to skip, called once per argumentRowId
    */
  def skip(morsel: Morsel, reserve: (S, Int) => Int): Unit

  /**
   * Filter the input morsel using the [[ArgumentState]] related to `argument`.
   *
   * @param onArgument is called once per argumentRowId, to generate a filter state.
   * @param onRow      is called once per row, and given the filter state of the current argumentRowId
   */
  def filterWithSideEffect[FILTER_STATE](morsel: Morsel,
                                         onArgument: (S, Int) => FILTER_STATE,
                                         onRow: (FILTER_STATE, ReadWriteRow) => Boolean): Unit

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
   * Initiate state and counting for a new argument
   *
   * @param argument                  the argument row id
   * @param argumentMorsel            the morsel that contains this argument
   * @param argumentRowIdsForReducers the argument row ids of reducers that correspond to the given argument.
   *                                  This is needed to decrement the reducers with the right IDs when
   *                                  we close an argument state. This is allowed to be null if these argument
   *                                  states are not closed or are not involved in a buffer with downstream
   *                                  reducers.
   */
  def initiate(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): Unit

  /**
   * Increment the argument counter for `argument`.
   */
  def increment(argument: Long): Unit

  /**
   * Decrement the argument counter for `argument`.
   * @return the argument state iff count has reached zero, otherwise `null`
   */
  def decrement(argument: Long): S

  /**
   * ID of this ArgumentStateMap
   */
  def argumentStateMapId: ArgumentStateMapId

  /**
   * Slot offset of the argument slot.
   */
  def argumentSlotOffset: Int
}

trait ArgumentStateMapWithoutArgumentIdCounter[S <: ArgumentState]extends ArgumentStateMap[S] {
  /**
   * Take the [[ArgumentState]] of one complete arguments. The [[ArgumentState]] will
   * be removed from the [[ArgumentStateMap]] and cannot be taken again or modified after this call.
   */
  def takeOneCompleted(): S
}

/**
 * This interface groups all methods that make use of the [[AbstractArgumentStateMap.lastCompletedArgumentId]]. We split this out so that
 * users of [[ArgumentStateMap]] do not accidentally mix calls with other methods.
 */
trait ArgumentStateMapWithArgumentIdCounter[S <: ArgumentState] extends ArgumentStateMap[S] {
  /**
   * When using this method to take argument states, an internal counter of the argument id of the last completed argument state is kept.
   * This will look at the next argument state and return that. If it is completed, it will take it and increment the counter.
   *
   * The counter is unaffected by other methods that take, so they should not be mixed!
   *
   * Returns an ArgumentStateWithCompleted of the [[ArgumentState]] for the specified argumentId
   * which indicated whether the state was completed or not,
   * or `null` if no state exist for that argumentId..
   */
  def takeNextIfCompletedOrElsePeek(): ArgumentStateWithCompleted[S]

  /**
   * Returns `true` if the next [[ArgumentState]] according to the internal counter mentioned in [[takeNextIfCompletedOrElsePeek()]]
   * is either completed or fulfills the given predicate.
   */
  def nextArgumentStateIsCompletedOr(statePredicate: S => Boolean): Boolean

  /**
   * Peeks at the next argument state according to the internal counter mentioned in [[takeNextIfCompletedOrElsePeek()]].
   */
  def peekNext(): S
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

    /**
     * The argument row ids of reducers for that correspond to argumentRowId.
     * This is needed to decrement the reducers with the right IDs when
     * we close an argument state. This is allowed to be null if these argument
     * states are not closed or are not involved in a buffer with downstream
     * reducers.
     */
    def argumentRowIdsForReducers: Array[Long]
  }

  /**
   * A state that keeps track whether work related to the argument id is cancelled.
   */
  trait WorkCanceller extends ArgumentState {
    def isCancelled: Boolean
  }

  /**
   * Accumulator of data. Has internal state which it updates using provided data.
   */
  trait MorselAccumulator[DATA <: AnyRef] extends ArgumentState {

    /**
     * Update internal state using the provided data.
     */
    def update(data: DATA): Unit
  }

  /**
   * Operators that use ArgumentStateMaps need to implement this to be able to instantiate
   * the corresponding ArgumentStateMap.
   */
  trait ArgumentStateFactory[S <: ArgumentState] {
    /**
     * Construct new ArgumentState for non-concurrent use.
     */
    def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): S

    /**
     * Construct new ArgumentState for concurrent use.
     */
    def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): S

    /**
     * True if argument states created by this factory should be completed on construction. This means that
     * the execution graph has been constructed in such a way that no updates will ever arrive to this
     * argument state.
     */
    def completeOnConstruction: Boolean = false
  }

  /**
   * Mapping of IDs to the corresponding ArgumentStateMap.
   */
  trait ArgumentStateMaps {
    /**
     * Get the argument state map for the given id.
     */
    def apply(argumentStateMapId: ArgumentStateMapId): ArgumentStateMap[_ <: ArgumentState]

    /**
     * This is used from generated code where interoperability with AnyVal (here ArgumentStateMapId) is limited
     */
    def applyByIntId(argumentStateMapId: Int): ArgumentStateMap[_ <: ArgumentState] = {
      apply(ArgumentStateMapId(argumentStateMapId))
    }
  }

  /**
   * One argument state together with the information whether it is completed, i.e.
   * all data has arrived.
   */
  case class ArgumentStateWithCompleted[S <: ArgumentState](argumentState: S, isCompleted: Boolean)

  def skip(morsel: Morsel, toSkip: Int): Unit = {
    val filteringMorsel = morsel.asInstanceOf[FilteringMorsel]

    if (filteringMorsel.hasCancelledRows) {
      val cursor = filteringMorsel.fullCursor()
      var i = 0L
      while (i < toSkip && cursor.next()) {
        filteringMorsel.cancelRow(cursor.row)
        i += 1
      }
    } else {
      val cursor = filteringMorsel.fullCursor(onFirstRow = true)
      filteringMorsel.cancelRows(cursor.row, cursor.row + toSkip)
    }
  }

  /**
   * Filter the rows of a morsel using a predicate, and redirect the morsel current row to
   * the rows new position.
   *
   * @param morsel the morsel to filter
   * @param predicate the predicate
   */
  def filter(morsel: Morsel,
             predicate: ReadWriteRow => Boolean): Unit = {
    val filteringMorsel = morsel.asInstanceOf[FilteringMorsel]

    if (filteringMorsel.hasCancelledRows) {
      filterLoop(filteringMorsel, predicate)
    } else {
      filterLoopRaw(filteringMorsel, predicate)
    }
  }

  private def filterLoop(filteringMorsel: FilteringMorsel,
                         predicate: ReadWriteRow => Boolean): Unit = {
    val cursor = filteringMorsel.fullCursor()
    while (cursor.next()) {
      if (!predicate(cursor)) {
        filteringMorsel.cancelRow(cursor.row)
      }
    }
  }

  // TODO: reenable optimization
  private def filterLoopRaw(filteringMorsel: FilteringMorsel,
                            predicate: ReadWriteRow => Boolean): Unit = {

    val cursor = filteringMorsel.fullCursor()
    while (cursor.next()) {
      if (!predicate(cursor)) {
        filteringMorsel.cancelRow(cursor.row)
      }
    }
  }

  def skip(argumentSlotOffset: Int,
           morsel: Morsel,
           reserve: (Long, Int) => Int): Unit = {
    val filteringMorsel = morsel.asInstanceOf[FilteringMorsel]
    if (filteringMorsel.hasCancelledRows) skipSlow(argumentSlotOffset, filteringMorsel, reserve)
    else skipFast(argumentSlotOffset, filteringMorsel, reserve)
  }

  /**
    * Does fast skipping by skipping a full range instead of doing it row by row. Can only be called
    * if we know there are no cancelled rows since otherwise we might skip an already skipped item and the number
    * of skipped items will be wrong
    */
  private def skipFast(argumentSlotOffset: Int,
                       filteringMorsel: FilteringMorsel,
                       reserve: (Long, Int) => Int): Unit = {
    val cursor = filteringMorsel.fullCursor(onFirstRow = true)
    while (cursor.onValidRow()) {
      val arg = ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset)
      val start: Int = cursor.row
      moveToEnd(argumentSlotOffset, cursor, arg)
      val end: Int = cursor.row
      val last = reserve(arg, end - start)
      filteringMorsel.cancelRows(start, start + last)
      //if moveToEnd didn't move use we now must move one step forwards
      if (end - start == 0) {
        cursor.next()
      }
    }

  }

  private def skipSlow(argumentSlotOffset: Int,
                       filteringMorsel: FilteringMorsel,
                       reserve: (Long, Int) => Int): Unit = {
    val cursor = filteringMorsel.fullCursor(onFirstRow = true)

    while (cursor.onValidRow()) {
      val arg = ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset)
      val start: Int = cursor.row
      var maxToSkip = 0
      while (cursor.onValidRow() && ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset) == arg) {
        cursor.next()
        maxToSkip += 1
      }
      val end = cursor.row
      cursor.setRow(start)

      val actualToSkip = reserve(arg, maxToSkip)
      var i = 0
      while (i < actualToSkip) {
        filteringMorsel.cancelRow(cursor.row)
        cursor.next()
        i += 1
      }

      cursor.setRow(end)
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
                           morsel: Morsel,
                           onArgument: (Long, Int) => FILTER_STATE,
                           onRow: (FILTER_STATE, ReadWriteRow) => Boolean): Unit = {
    val filteringMorsel = morsel.asInstanceOf[FilteringMorsel]

    val cursor = filteringMorsel.fullCursor(onFirstRow = true)

    while (cursor.onValidRow()) {
      val arg = ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset)
      val start: Int = cursor.row
      moveToEnd(argumentSlotOffset, cursor, arg)
      val end: Int = cursor.row
      cursor.setRow(start)

      val filterState = onArgument(arg, end - start)
      while (cursor.row < end) {
        if (!onRow(filterState, cursor)) {
          filteringMorsel.cancelRow(cursor.row)
        }
        cursor.next()
      }
    }
  }

  case class PerArgument[T](argumentRowId: Long, value: T)

  def map[T](argumentSlotOffset: Int,
             morsel: Morsel,
             f: Morsel => T): IndexedSeq[PerArgument[T]] = {

    val result = new ArrayBuffer[PerArgument[T]]()
    foreach(argumentSlotOffset,
      morsel,
      (argumentRowId, view) => result += PerArgument(argumentRowId, f(view)))
    result
  }

  def foreach[T](argumentSlotOffset: Int,
                 morsel: Morsel,
                 f: (Long, Morsel) => Unit): Unit = {

    val cursor = morsel.readCursor(onFirstRow = true)

    while (cursor.onValidRow()) {
      val arg = ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset)
      val start: Int = cursor.row
      while (cursor.onValidRow && ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset) == arg) {
        cursor.next()
      }
      val end: Int = cursor.row

      val view = morsel.view(start, end)
      f(arg, view)
    }
  }

  private def moveToEnd(argumentSlotOffset: Int,
                        cursor: MorselFullCursor,
                        arg: Long): Unit = {
    while (cursor.onValidRow() && ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset) == arg) {
      cursor.next()
    }
  }
}
