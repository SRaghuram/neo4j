/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.FilteringMorsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.memory.MemoryTracker

import scala.collection.mutable.ArrayBuffer

/**
 * Maps every argument to one `ArgumentState`/`S`.
 *
 * An ArgumentStateMap can be either ordered or unordered, and the semantics of some methods change depending on that.
 *
 * The semantics for ordered argument state maps are such that certain methods will always interact with the next argument
 * state according to argument row if. After an argument state is taken, "next" moves by one.
 *
 * The semantics for unordered argument state maps are such that certain methods will always interact with _some_ argument state.
 *
 * These semantics help us to
 * a) make progress as fast as possible if no ordering by argument row id is needed.
 * b) get data in the right argument row id order if needed.
 */
trait ArgumentStateMap[S <: ArgumentState] {

  /**
   * Try to take all the argument states and apply the given function to all the ones which could be taken.
   *
   * This function will break any ordering guarantees from ordered ArgumentStateMaps.
   *
   * @param f the function to apply to all taken argument states.
   */
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
   * @param createState is called once per argumentRowId, returns filter state and a flag that indicates if this argument row id is completed and this was
   *                    the last time we needed to create FILTER_STATE for this argument row id.
   * @param predicate   is called once per row, and given the filter state of the current argumentRowId
   */
  def filterWithSideEffect[FILTER_STATE](morsel: Morsel,
                                         createState: (S, Int) => FilterStateWithIsLast[FILTER_STATE],
                                         predicate: (FILTER_STATE, ReadWriteRow) => Boolean): Unit

  /**
   * Removes the state of this argument.
   * @return the state if the argument was removed, `null` if it had been removed earlier.
   **/
  def remove(argument: Long): S

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
   * @param initialCount              the initial count for the argument row id
   */
  def initiate(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], initialCount: Int): Unit

  /**
   * Increment the argument counter for `argument`.
   */
  def increment(argument: Long): Unit

  /**
   * Decrement the argument counter for `argument`.
   *
   * WARNING: The caller does not get ownership or exclusive access to the state
   *          and it may concurrently be taken and updated or closed by another thread
   *
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

  /**
   * The semantics of this call differ depending on whether this ASM is ordered.
   *
   * Ordered:
   * Returns the [[ArgumentState]] for up to the next n argumentIds, as long as they are completed.
   * If the next argument state is not completed, returns `null`.
   *
   * Unordered:
   * Take the [[ArgumentState]] of n complete arguments. The [[ArgumentState]] will
   * be removed from the [[ArgumentStateMap]] and cannot be taken again or modified after this call.
    *
    * @param n the maximum number of arguments to take
   */
  def takeCompleted(n: Int): IndexedSeq[S]

  /**
   * The semantics of this call differ depending on whether this ASM is ordered.
   *
   * Ordered:
   * Look at the next [[ArgumentState]] by argumentId.
   *
   * Returns an [[ArgumentStateWithCompleted]] of that [[ArgumentState]]
   * which indicates whether the state was completed or not,
   * or `null` if no state exist. The [[ArgumentState]] will
   * be removed from the [[ArgumentStateMap]] and cannot be taken again or modified after this call.
   *
   * Unordered:
   * Look at one [[ArgumentState]].
   *
   * Returns an [[ArgumentStateWithCompleted]] of that [[ArgumentState]]
   * which indicates whether the state was completed or not,
   * or `null` if no state exist. The [[ArgumentState]] will
   * be removed from the [[ArgumentStateMap]] and cannot be taken again or modified after this call.
   */
  def takeOneIfCompletedOrElsePeek(): ArgumentStateWithCompleted[S]

  /**
   * Like [[takeOneIfCompletedOrElsePeek]] but restricted to a single, specific argument.
   */
  def takeIfCompletedOrElsePeek(argumentId: Long): ArgumentStateWithCompleted[S]

  /**
   * The semantics of this call differ depending on whether this ASM is ordered.
   *
   * Ordered:
   * Returns `true` if the next [[ArgumentState]] is either completed or fulfills the given predicate.
   *
   * Unordered:
   * Returns `true` if the some [[ArgumentState]] is either completed or fulfills the given predicate.
   */
  def someArgumentStateIsCompletedOr(statePredicate: S => Boolean): Boolean

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
   * Returns `true` if the some [[ArgumentState]] fulfills the given predicate.
   */
  def exists(statePredicate: S => Boolean): Boolean
}

/**
 * Static methods and interfaces connected to ArgumentStateMap
 */
object ArgumentStateMap {

  /**
   * State that belongs to one argumentRowId and is kept in the [[ArgumentStateMap]]
   */
  trait ArgumentState extends AutoCloseable {
    /**
     * The ID of the argument row for this state.
     */
    def argumentRowId: Long

    /**
     * The shallow size of the ArgumentStateMap
     */
    def shallowSize: Long

    /**
     * The argument row ids of reducers for that correspond to argumentRowId.
     * This is needed to decrement the reducers with the right IDs when
     * we close an argument state. This is allowed to be null if these argument
     * states are not closed or are not involved in a buffer with downstream
     * reducers.
     */
    def argumentRowIdsForReducers: Array[Long]

    override def close(): Unit = {}
  }

  /**
   * A state that keeps track whether work related to the argument id is cancelled.
   */
  trait WorkCanceller extends ArgumentState {
    /**
     * @return `true` if cancelled otherwise `false`
     */
    def isCancelled: Boolean

    /**
     * @return the number of remaining rows before work is done (cancelled)
     */
    def remaining: Long
  }

  /**
   * Accumulator of data. Has internal state which it updates using provided data.
   */
  trait MorselAccumulator[DATA <: AnyRef] extends ArgumentState {

    /**
     * Update internal state using the provided data.
     */
    def update(data: DATA, resources: QueryResources): Unit

    /**
     * Create a read cursor over a snapshot of all morsels in this accumulator
     */
    def readCursor(onFirstRow: Boolean = false): MorselReadCursor = ???
  }

  /**
   * Operators that use ArgumentStateMaps need to implement this to be able to instantiate
   * the corresponding ArgumentStateMap.
   */
  abstract class ArgumentStateFactory[S <: ArgumentState] {
    /**
     * Construct new ArgumentState for non-concurrent use.
     */
    def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker): S

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

  trait ArgumentStateBufferFactoryFactory extends ArgumentStateFactoryFactory[ArgumentStateBuffer]

  trait ArgumentStateFactoryFactory[S <: ArgumentState] {
    def createFactory(stateFactory: StateFactory, operatorId: Int): ArgumentStateFactory[S]
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
      // if moveToEnd didn't move us, we now must move one step forwards
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
   * For each argument row id at `argumentSlotOffset`, create a filter state (`FILTER_STATE`).
   *
   * - If filter state is not null, it is used to call `predicate` on each row with the argument row id. If `predicate`
   *   returns true, the row is retained, otherwise it's discarded.
   * - If filter state is null, all rows with the the argument row id are discarded.
   *
   * @param morsel the morsel to filter
   * @param createState createState(argumentRowId, nRows): FilterState
   * @param predicate predicate(FilterState, morselRow): Boolean
   * @tparam FILTER_STATE state used for filtering
   */
  def filter[FILTER_STATE](argumentSlotOffset: Int,
                           morsel: Morsel,
                           createState: (Long, Int) => FILTER_STATE,
                           predicate: (FILTER_STATE, ReadWriteRow) => Boolean): Unit = {
    val filteringMorsel = morsel.asInstanceOf[FilteringMorsel]

    val cursor = filteringMorsel.fullCursor(onFirstRow = true)

    while (cursor.onValidRow()) {
      val arg = ArgumentSlots.getArgumentAt(cursor, argumentSlotOffset)
      val start: Int = cursor.row
      moveToEnd(argumentSlotOffset, cursor, arg)
      val end: Int = cursor.row
      cursor.setRow(start)

      val filterState = createState(arg, end - start)
      while (cursor.row < end) {
        if (filterState == null || !predicate(filterState, cursor)) { // Null check needs to be first
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

    if (argumentSlotOffset == TopLevelArgument.SLOT_OFFSET) {
      singletonIndexedSeq(PerArgument(TopLevelArgument.VALUE, f(morsel)))
    } else {
      val result = new ArrayBuffer[PerArgument[T]]()
      foreach(argumentSlotOffset,
              morsel,
              (argumentRowId, view) => result += PerArgument(argumentRowId, f(view)))
      result
    }
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

/**
 * Holds generic filter state for a specific argument row (that is not specified in this context) and a flag that indicates if this was the last time we needed
 * to create such filter state.
 *
 * @param filterState generic filter state for an argument row
 * @param isLast true if this filter state is the last needed for the argument row
 * @tparam FILTER_STATE filter state type
 */
case class FilterStateWithIsLast[FILTER_STATE](filterState: FILTER_STATE, isLast: Boolean)
