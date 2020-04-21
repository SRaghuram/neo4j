/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.ScopedMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues

/**
 * Aggregator for collect(...).
 */
case object CollectAggregator extends Aggregator {
  override def newUpdater(memoryTracker: MemoryTracker): Updater = new CollectUpdater(preserveNulls = false)
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new CollectStandardReducer(memoryTracker)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer()
}

case object CollectAllAggregator extends Aggregator {
  override def newUpdater(memoryTracker: MemoryTracker): Updater = new CollectUpdater(preserveNulls = true)
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new CollectStandardReducer(memoryTracker)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer()
}

case object CollectDistinctAggregator extends Aggregator {
  override def newUpdater(memoryTracker: MemoryTracker): Updater = new OrderedDistinctUpdater(memoryTracker)
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new OrderedDistinctStandardReducer(DummyReducer, memoryTracker)
  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(DummyReducer)
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

object DummyReducer extends DistinctInnerReducer {
  override def update(value: AnyValue): Unit = ()
  override def result: AnyValue = throw new IllegalStateException("Must be used inside of CollectDistinctReducer")
}

class OrderedDistinctStandardReducer(inner: DistinctInnerReducer, memoryTracker: MemoryTracker) extends DistinctReducer(inner) {
  private val seenSet = new java.util.LinkedHashSet[AnyValue]() // TODO: Use a heap tracking ordered distinct set
  private val scopedMemoryTracker = new ScopedMemoryTracker(memoryTracker)

  override protected def seen(e: AnyValue): Boolean = {
    val added = seenSet.add(e)
    if (added) {
      scopedMemoryTracker.allocateHeap(e.estimatedHeapUsage())
    }
    added
  }

  override def result: AnyValue = {
    val collection = ListValueBuilder.newListBuilder()
    seenSet.forEach(value => collection.add(value))
    collection.build()
  }

  override def close(): Unit = {
    scopedMemoryTracker.close()
    super.close()
  }
}
