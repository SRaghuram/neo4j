/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
 * Aggregator for min(...).
 */
case object MinAggregator extends Aggregator {

  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new MinStandardReducer
  override def newConcurrentReducer: Reducer = new MinConcurrentReducer

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[MinStandardReducer])

  def shouldUpdate(min: AnyValue, value: AnyValue): Boolean =
    !(value eq Values.NO_VALUE) && ((min eq Values.NO_VALUE) || AnyValues.COMPARATOR.compare(min, value) > 0)
}

class MinUpdater {
  private[aggregators] var min: AnyValue = Values.NO_VALUE
  protected def update(value: AnyValue): Unit =
    if (MinAggregator.shouldUpdate(min, value))
      min = value
}

class MinStandardReducer() extends MinUpdater with DirectStandardReducer {
  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue = min

  // Updater
  override def add(value: Array[AnyValue]): Unit = update(value(0))
}

class MinConcurrentReducer() extends Reducer {
  private val minAR = new AtomicReference[AnyValue](Values.NO_VALUE)

  // Reducer
  override def newUpdater(): Updater = new Upd()
  override def result: AnyValue = minAR.get

  // Updater
  class Upd() extends MinUpdater with Updater {
    override def add(value: Array[AnyValue]): Unit = update(value(0))
    override def applyUpdates(): Unit =
      minAR.updateAndGet(oldMin => if (MinAggregator.shouldUpdate(oldMin, min)) min else oldMin)
  }
}
