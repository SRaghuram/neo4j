/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.HeapTrackingListValueBuilder
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues

/**
 * Aggregator for collect(...).
 */
case object CollectAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new CollectStandardReducer(preserveNulls = false, memoryTracker)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer(preserveNulls = false)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[CollectStandardReducer])
}

/**
 * Aggregator for collect(...) including nulls.
 */
case object CollectAllAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new CollectStandardReducer(preserveNulls = true, memoryTracker)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer(preserveNulls = true)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[CollectStandardReducer])
}

/**
 * Aggregator for collect(DISTINCT ...). Uses specialized reducers to avoid keeping both a set and list in memory.
 */
case object CollectDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new CollectDistinctStandardReducer(memoryTracker)
  override def newConcurrentReducer: Reducer = new CollectDistinctConcurrentReducer()

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[CollectDistinctStandardReducer])
}

abstract class CollectUpdater(preserveNulls: Boolean, memoryTracker: MemoryTracker) {
  private[aggregators] var collection = HeapTrackingListValueBuilder.newHeapTrackingListBuilder(memoryTracker)
  protected def collect(value: AnyValue): Boolean = {
    val addMe = preserveNulls || !(value eq Values.NO_VALUE)
    if (addMe) {
      collection.add(value)
    }
    addMe
  }
  protected def reset(): Unit = {
    collection.close()
    collection = HeapTrackingListValueBuilder.newHeapTrackingListBuilder(memoryTracker)
  }
}

class CollectStandardReducer(preserveNulls: Boolean, memoryTracker: MemoryTracker) extends CollectUpdater(preserveNulls, memoryTracker) with DirectStandardReducer {

  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue = {
    collection.buildAndClose()
  }

  // Updater
  override def add(value: AnyValue): Unit = {
    collect(value)
  }
}

class CollectConcurrentReducer(preserveNulls: Boolean) extends Reducer {
  private val collections = new ConcurrentLinkedQueue[ListValue]()

  override def newUpdater(): Updater = new Upd()
  override def result: AnyValue = VirtualValues.concat(collections.toArray(new Array[ListValue](0)):_*)

  class Upd() extends CollectUpdater(preserveNulls, EmptyMemoryTracker.INSTANCE) with Updater {
    override def add(value: AnyValue): Unit = collect(value)
    override def applyUpdates(): Unit = {
      val listValue = collection.buildAndClose()
      collections.add(listValue)
      reset()
    }
  }
}

class CollectDistinctStandardReducer(memoryTracker: MemoryTracker) extends DirectStandardReducer {
  // NOTE: The owner is responsible for closing the given memory tracker in the right scope, so we do not need to use a ScopedMemoryTracker
  //       or close the seenSet explicitly here.
  private val seenSet = HeapTrackingCollections.newSet[AnyValue](memoryTracker)
  private val collection = HeapTrackingListValueBuilder.newHeapTrackingListBuilder(memoryTracker)

  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue = {
    collection.buildAndClose()
  }

  // Updater
  override def add(value: AnyValue): Unit = {
    if (!(value eq Values.NO_VALUE)) {
      val isUnique = seenSet.add(value)
      if (isUnique) {
        collection.add(value)
      }
    }
  }
}

class CollectDistinctConcurrentReducer() extends Reducer {
  private val seenSet = ConcurrentHashMap.newKeySet[AnyValue]()

  override def newUpdater(): Updater = new Upd()
  override def result: AnyValue = {
    val collection = ListValueBuilder.newListBuilder()
    seenSet.forEach(value => collection.add(value))
    collection.build()
  }

  class Upd() extends Updater {
    private val partSeenSet: DistinctSet[AnyValue] = DistinctSet.createDistinctSet[AnyValue](EmptyMemoryTracker.INSTANCE)

    override def add(value: AnyValue): Unit = {
      if (!(value eq Values.NO_VALUE))
        partSeenSet.add(value)
    }

    override def applyUpdates(): Unit = {
      partSeenSet.each(x => {
        seenSet.add(x)
      })
    }
  }
}
