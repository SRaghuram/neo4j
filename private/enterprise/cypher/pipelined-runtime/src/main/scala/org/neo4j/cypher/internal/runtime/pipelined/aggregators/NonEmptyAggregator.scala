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
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new NonEmptyStandardReducer()

  override def newConcurrentReducer: Reducer = new NonEmptyConcurrentReducer()

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[NonEmptyStandardReducer])
}

class NonEmptyStandardReducer() extends DirectStandardReducer {
  private var nonEmpty = false

  // Reducer
  override def newUpdater(): Updater = this

  override def result: AnyValue = Values.booleanValue(nonEmpty)

  // Updater
  override def add(value: AnyValue): Unit =
    nonEmpty = true
}

class NonEmptyConcurrentReducer() extends Reducer {
  @volatile private var nonEmpty = false

  override def newUpdater(): Updater = new Upd()

  override def result: AnyValue = Values.booleanValue(nonEmpty)

  class Upd() extends Updater {
    override def add(value: AnyValue): Unit =
      nonEmpty = true

    override def applyUpdates(): Unit = {}
  }

}
