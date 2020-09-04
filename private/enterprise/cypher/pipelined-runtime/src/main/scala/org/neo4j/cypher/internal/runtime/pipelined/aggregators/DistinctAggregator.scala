/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

class DistinctStandardReducer(inner: StandardReducer, memoryTracker: MemoryTracker) extends StandardReducer {
  // NOTE: The owner is responsible for closing the given memory tracker in the right scope, so we do not need to close the seenSet explicitly
  private val seenSet: DistinctSet[AnyValue] = DistinctSet.createDistinctSet(memoryTracker)

  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue = inner.result

  // Updater
  override def add(values: Array[AnyValue]): Unit = {
    val value = values(0)
    if (seenSet.add(value)) {
      inner.add(values)
    }
  }

  override def isDirect: Boolean = inner.isDirect
  override def applyUpdates(): Unit = inner.applyUpdates()
}

class DistinctConcurrentReducer(inner: Reducer) extends Reducer {
  private val seenSet = ConcurrentHashMap.newKeySet[AnyValue]()

  override def newUpdater(): Updater = new Upd(inner.newUpdater())
  override def result: AnyValue = inner.result

  class Upd(inner: Updater) extends Updater {
    private var partSeenSet: DistinctSet[AnyValue] = DistinctSet.createDistinctSet[AnyValue](EmptyMemoryTracker.INSTANCE)

    override def add(value: Array[AnyValue]): Unit = partSeenSet.add(value(0))
    override def applyUpdates(): Unit = {
      partSeenSet.each(x => {
        if (seenSet.add(x)) {
          inner.add(Array(x))
        }
      })
      partSeenSet = DistinctSet.createDistinctSet[AnyValue](EmptyMemoryTracker.INSTANCE)
      inner.applyUpdates()
    }
  }
}
