/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

/**
 * Computational parallel primitive which allows aggregating data
 * over many input rows. Creates [[Reducer]]s, which perform specific
 * reducing operations.
 *
 * To update a [[Reducer]] with new values, an [[Updater]] is acquired.
 * Value are added to the [[Updater]] using [[Updater.add()]], which is
 * guaranteed to be free from synchronization. The state of the Updater
 * is applied to the reducer on [[Updater.applyUpdates()]], which will
 * perform any necessary synchronization with other worker threads.
 */
trait Aggregator {
  def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer
  def newConcurrentReducer: Reducer

  def isDirect: Boolean = newStandardReducer(EmptyMemoryTracker.INSTANCE).isDirect
  def standardShallowSize: Long
}

object Aggregator {
  def allDirect(aggregators: Array[Aggregator]): Boolean = {
    var i = 0
    while (i < aggregators.length) {
      if (!aggregators(i).isDirect)
        return false
      i += 1
    }
    true
  }
}

/**
 * Performs the initial parts of an aggregation that can be done
 * without synchronization.
 */
trait Updater {

  def initialize(state: QueryState): Unit = {}
  /**
   * Update this updater with a new value.
   *
   * NOTE: this is a specialization and should
   * only be used for aggregations functions taking
   * a single input value
   */
  def add(value: AnyValue): Unit

  /**
   * Update this updater with new values.
   */
  def add(values: Array[AnyValue]): Unit = add(values(0))

  /**
   * Apply the current state of this updater to it's parent [[Reducer]].
   *
   * This will reset this updater to it's original state.
   */
  def applyUpdates(): Unit
}

/**
 * Performs the final parts of an aggregation that might require
 * synchronization.
 */
trait Reducer {
  /**
   * Create a new [[Updater]] for updating this reducer.
   */
  def newUpdater(): Updater

  /**
   * Compute the result of this reducer.
   */
  def result: AnyValue
}

/**
 * In standard (single-threaded) execution, reducers and updaters are the same object,
 * to minimize object creation and unnecessary work doubling between the updater and reducer.
 */
trait StandardReducer extends Reducer with Updater {

  /**
   * See [[DirectStandardReducer]].
   */
  def isDirect: Boolean = false
}

/**
 * A DirectStandardReducer updates the reducer directly in [[Updater.add()]], and does nothing on
 * [[Updater.applyUpdates()]]. This means that it is safe to not call [[Updater.applyUpdates()]] and
 * still get the same result.
 */
trait DirectStandardReducer extends StandardReducer {
  override final def isDirect: Boolean = true
  override final def applyUpdates(): Unit = {}
}
