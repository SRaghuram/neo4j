/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateWithCompleted}

import scala.collection.mutable.ArrayBuffer

/**
  * Maps every argument to one `ArgumentState`/`S`.
  */
trait ArgumentStateMap[S <: ArgumentState] {

  def clearAll(f: S => Unit): Unit

  def update(argumentRowId: Long, onState: S => Unit): Unit

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
    * @param argument       the argument row id
    * @param argumentMorsel the morsel that contains this argument
    * @param argumentRowIdsForReducers the argument row ids of reducers for that correspond to the given argument.
    *                                  This is needed to decrement the reducers with the right IDs when
    *                                  we close an argument state. This is allowed to be null if these argument
    *                                  states are not closed or are not involved in a buffer with downstream
    *                                  reducers.
    */
  def initiate(argument: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): Unit

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
    def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): S

    def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): S
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
    * One argument state together with the information whether it is completed, i.e.
    * all data has arrived.
    */
  case class ArgumentStateWithCompleted[S <: ArgumentState](argumentState: S, isCompleted: Boolean)

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

    if (newCurrentRow == -1 && morsel.getCurrentRow >= morsel.getValidRows) {
      newCurrentRow = writingRow.getCurrentRow
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

    if (newCurrentRow == -1 && morsel.getCurrentRow >= morsel.getValidRows) {
      newCurrentRow = writingRow.getCurrentRow
    }

    morsel.moveToRow(newCurrentRow)
    morsel.finishedWritingUsing(writingRow)
    cancelled
  }

  case class PerArgument[T](argumentRowId: Long, value: T)

  def map[T](argumentSlotOffset: Int,
             morsel: MorselExecutionContext,
             f: MorselExecutionContext => T): IndexedSeq[PerArgument[T]] = {

    val readingRow = morsel.shallowCopy()
    readingRow.resetToFirstRow()
    val result = new ArrayBuffer[PerArgument[T]]()

    while (readingRow.isValidRow) {
      val arg = readingRow.getLongAt(argumentSlotOffset)
      val start: Int = readingRow.getCurrentRow
      while (readingRow.isValidRow && readingRow.getLongAt(argumentSlotOffset) == arg) {
        readingRow.moveToNextRow()
      }
      val end: Int = readingRow.getCurrentRow

      val view = readingRow.view(start, end)
      result += PerArgument(arg, f(view))
    }
    result
  }
}
