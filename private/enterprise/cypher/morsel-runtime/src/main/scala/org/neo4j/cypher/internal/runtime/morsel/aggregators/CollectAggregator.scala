/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.aggregators

import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{ListValue, VirtualValues}


/**
  * Aggregator for collect(...).
  */
case object CollectAggregator extends Aggregator {

  override def newUpdater: Updater = new CollectUpdater
  override def newStandardReducer: Reducer = new CollectStandardReducer
  override def newConcurrentReducer: Reducer = new CollectConcurrentReducer
}

class CollectUpdater() extends Updater {
  private[aggregators] val collection = new util.ArrayList[AnyValue]()
  override def update(value: AnyValue): Unit =
    if (!(value eq Values.NO_VALUE)) {
      collection.add(value)
    }
}

class CollectStandardReducer() extends Reducer {
  private val collections = Array.newBuilder[ListValue]
  override def update(updater: Updater): Unit =
    updater match {
      case u: CollectUpdater =>
        collections += VirtualValues.fromList(u.collection)
    }

  override def result: AnyValue = VirtualValues.concat(collections.result():_*)
}

class CollectConcurrentReducer() extends Reducer {
  private val collections = new ConcurrentLinkedQueue[AnyValue]()

  override def update(updater: Updater): Unit =
    updater match {
      case u: CollectUpdater =>
        collections.add(VirtualValues.fromList(u.collection))
    }

  override def result: AnyValue = VirtualValues.concat(collections.toArray(new Array[ListValue](0)):_*)
}
