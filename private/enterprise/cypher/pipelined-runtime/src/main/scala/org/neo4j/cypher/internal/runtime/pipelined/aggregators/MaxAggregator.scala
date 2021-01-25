/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.Values

/**
 * Aggregator for max(...).
 */
case object MaxAggregator extends Aggregator {

  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new MaxStandardReducer
  override def newConcurrentReducer: Reducer = new MaxConcurrentReducer

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[MaxStandardReducer])

  def shouldUpdate(max: AnyValue, value: AnyValue): Boolean =
    !(value eq Values.NO_VALUE) && ((max eq Values.NO_VALUE) || AnyValues.COMPARATOR.compare(max, value) < 0)
}

class MaxUpdater {
  private[aggregators] var max: AnyValue = Values.NO_VALUE

  protected def update(value: AnyValue): Unit =
    if (MaxAggregator.shouldUpdate(max, value))
      max = value
}

class MaxStandardReducer() extends MaxUpdater with DirectStandardReducer {
  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue = max

  // Updater
  override def add(value: AnyValue): Unit = update(value)
}

class MaxConcurrentReducer() extends Reducer {
  // Reducer
  private val maxAR = new AtomicReference[AnyValue](Values.NO_VALUE)

  override def newUpdater(): Updater = new Upd()
  override def result: AnyValue = maxAR.get

  // Updater
  class Upd() extends MaxUpdater with Updater {
    override def add(value: AnyValue): Unit = update(value)
    override def applyUpdates(): Unit =
      maxAR.updateAndGet(oldMax => if (MaxAggregator.shouldUpdate(oldMax, max)) max else oldMax)
  }
}
