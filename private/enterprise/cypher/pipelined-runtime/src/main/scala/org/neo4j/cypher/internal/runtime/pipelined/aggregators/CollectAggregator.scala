/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable.ArrayBuffer

/**
 * Aggregator for collect(...).
 */
case object CollectAggregator extends Aggregator {

  override def newUpdater: Updater = new CollectUpdater(preserveNulls = false)
  override def newStandardReducer(memoryTracker: QueryMemoryTracker, operatorId: Id): Reducer = new CollectStandardReducer(memoryTracker, operatorId)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer()
}

case object CollectAllAggregator extends Aggregator {

  override def newUpdater: Updater = new CollectUpdater(preserveNulls = true)
  override def newStandardReducer(memoryTracker: QueryMemoryTracker, operatorId: Id): Reducer = new CollectStandardReducer(memoryTracker, operatorId)
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer()
}

class CollectUpdater(preserveNulls: Boolean) extends Updater {
  private[aggregators] val collection = ListValueBuilder.newListBuilder()
  override def update(value: AnyValue): Unit =
    if (preserveNulls || !(value eq Values.NO_VALUE)) {
      collection.add(value)
    }
}

class CollectStandardReducer(memoryTracker: QueryMemoryTracker, operatorId: Id) extends Reducer {
  private val collections = new ArrayBuffer[ListValue]
  override def update(updater: Updater): Unit = {
    updater match {
      case u: CollectUpdater =>
        val value = u.collection.build();
        collections += value
        // Note: this allocation is currently never de-allocated
        memoryTracker.allocated(value, operatorId.x)
    }
  }

  override def result: AnyValue = VirtualValues.concat(collections: _*)
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
