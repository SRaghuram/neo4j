/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

/**
 * Aggregator for count(...).
 */
case object CountAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new CountStandardReducer(countNulls = false)
  override def newConcurrentReducer: Reducer = new CountConcurrentReducer(countNulls = false)
}

/**
 * Aggregator for count(DISTINCT..).
 */
case object CountDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new DistinctStandardReducer(new CountStandardReducer(countNulls = false), memoryTracker)
  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new CountConcurrentReducer(countNulls = false))
}

/**
 * Aggregator for count(*).
 */
case object CountStarAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new CountStandardReducer(countNulls = true)
  override def newConcurrentReducer: Reducer = new CountConcurrentReducer(countNulls = true)
}

class CountStandardReducer(countNulls: Boolean) extends DirectStandardReducer {
  private var count = 0L

  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue = Values.longValue(count)

  // Updater
  override def add(value: AnyValue): Unit =
    if (countNulls || !(value eq Values.NO_VALUE))
      count += 1
}

class CountConcurrentReducer(countNulls: Boolean) extends Reducer {
  private val count = new AtomicLong(0L)

  override def newUpdater(): Updater = new Upd(countNulls)
  override def result: AnyValue = Values.longValue(count.get)

  class Upd(countNulls: Boolean) extends Updater {
    var partCount = 0L
    override def add(value: AnyValue): Unit =
      if (countNulls || !(value eq Values.NO_VALUE))
        partCount += 1

    override def applyUpdates(): Unit = {
      count.addAndGet(partCount)
      partCount = 0L
    }
  }
}
