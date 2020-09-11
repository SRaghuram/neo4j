/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

/**
 * Aggregator for NonEmpty(...).
 */
case object NonEmptyAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new CheckEmptyStandardReducer(false)

  override def newConcurrentReducer: Reducer = new CheckEmptyConcurrentReducer(false)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[CheckEmptyStandardReducer])
}

/**
 * Aggregator for IsEmpty(...).
 */
case object IsEmptyAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new CheckEmptyStandardReducer(true)

  override def newConcurrentReducer: Reducer = new CheckEmptyConcurrentReducer(true)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[CheckEmptyStandardReducer])
}

class CheckEmptyStandardReducer(shouldBeEmpty: Boolean) extends DirectStandardReducer {
  private var isEmpty = true

  // Reducer
  override def newUpdater(): Updater = this

  override def result: AnyValue = Values.booleanValue(isEmpty == shouldBeEmpty)

  // Updater
  override def add(value: AnyValue): Unit =
    isEmpty = false
}

class CheckEmptyConcurrentReducer(shouldBeEmpty: Boolean) extends Reducer {
  @volatile private var isEmpty = true

  override def newUpdater(): Updater = new Upd()

  override def result: AnyValue = Values.booleanValue(isEmpty == shouldBeEmpty)

  class Upd() extends Updater {
    override def add(value: AnyValue): Unit =
      isEmpty = false

    override def applyUpdates(): Unit = {}
  }

}
