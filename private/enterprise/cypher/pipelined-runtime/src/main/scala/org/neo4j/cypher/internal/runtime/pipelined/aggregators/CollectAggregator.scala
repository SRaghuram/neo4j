/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues

/**
 * Aggregator for collect(...).
 */
case object CollectAggregator extends Aggregator {
  override def newUpdater: Updater = new CollectUpdater(preserveNulls = false)
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new CollectStandardReducer(memoryTracker)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer()
}

case object CollectAllAggregator extends Aggregator {
  override def newUpdater: Updater = new CollectUpdater(preserveNulls = true)
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new CollectStandardReducer(memoryTracker)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer()
}

case object CollectDistinctAggregator extends Aggregator {
  override def newUpdater: Updater = new DistinctInOrderUpdater
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new DistinctInOrderStandardReducer(new MemoryTrackingReducer(memoryTracker)) with CollectDistinctReducer
  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new DummyReducer) with CollectDistinctReducer
}

class CollectUpdater(preserveNulls: Boolean) extends Updater {
  private[aggregators] val collection = ListValueBuilder.newListBuilder()
  override def update(value: AnyValue): Unit =
    if (preserveNulls || !(value eq Values.NO_VALUE)) {
      collection.add(value)
    }
}

class CollectStandardReducer(memoryTracker: MemoryTracker) extends Reducer {
  private val collection = ListValueBuilder.newListBuilder()
  override def update(updater: Updater): Unit = {
    updater match {
      case u: CollectUpdater =>
        collection.combine(u.collection)
        // Note: this allocation is currently never de-allocated
        memoryTracker.allocateHeap(u.collection.estimatedHeapUsage())
    }
  }

  override def result: AnyValue =
    collection.build()
}

class CollectConcurrentReducer() extends Reducer {
  private val collections = new ConcurrentLinkedQueue[ListValue]()

  override def update(updater: Updater): Unit =
    updater match {
      case u: CollectUpdater =>
        collections.add(u.collection.build())
    }

  override def result: AnyValue = VirtualValues.concat(collections.toArray(new Array[ListValue](0)):_*)
}

class MemoryTrackingReducer(memoryTracker: MemoryTracker) extends DistinctInnerReducer {
  override def update(value: AnyValue): Unit = memoryTracker.allocateHeap(value.estimatedHeapUsage)
  override def result: AnyValue = throw new IllegalStateException("Must be used inside of CollectDistinctReducer")
}

class DummyReducer extends DistinctInnerReducer {
  override def update(value: AnyValue): Unit = ()
  override def result: AnyValue = throw new IllegalStateException("Must be used inside of CollectDistinctReducer")
}

trait CollectDistinctReducer {
  self: DistinctReducer =>

  override def result: AnyValue = {
    val collection = ListValueBuilder.newListBuilder()
    seen.forEach(value => collection.add(value))
    collection.build()
  }
}

class DistinctInOrderStandardReducer(inner: DistinctInnerReducer) extends DistinctReducer(inner) {
  override protected val seen: java.util.Set[AnyValue] = new java.util.LinkedHashSet[AnyValue]()
}

class DistinctInOrderUpdater() extends DistinctUpdater {
  override val seen: java.util.Set[AnyValue] = new java.util.LinkedHashSet[AnyValue]()
}
