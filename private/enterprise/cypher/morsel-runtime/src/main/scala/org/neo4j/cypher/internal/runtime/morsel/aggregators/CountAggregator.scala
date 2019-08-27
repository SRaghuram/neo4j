/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.aggregators
import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values


/**
  * Aggregator for count(...).
  */
case object CountAggregator extends Aggregator {

  override def newUpdater: Updater = new CountUpdater
  override def newStandardReducer(memoryTracker: QueryMemoryTracker): Reducer = new CountStandardReducer
  override def newConcurrentReducer: Reducer = new CountConcurrentReducer

  class CountUpdater() extends CountUpdaterBase {
    override def update(value: AnyValue): Unit =
      if (!(value eq Values.NO_VALUE))
        count += 1
  }
}

/**
  * Aggregator for count(*).
  */
case object CountStarAggregator extends Aggregator {

  override def newUpdater: Updater = new CountStarUpdater
  override def newStandardReducer(memoryTracker: QueryMemoryTracker): Reducer = new CountStandardReducer
  override def newConcurrentReducer: Reducer = new CountConcurrentReducer

  class CountStarUpdater() extends CountUpdaterBase {
    override def update(value: AnyValue): Unit = count += 1
  }
}

abstract class CountUpdaterBase extends Updater {
  private[aggregators] var count = 0L
}

class CountStandardReducer() extends Reducer {
  private var count = 0L

  override def update(updater: Updater): Unit =
    updater match {
      case u: CountUpdaterBase =>
        count += u.count
    }

  override def result: AnyValue = Values.longValue(count)
}

class CountConcurrentReducer() extends Reducer {
  private val count = new AtomicLong(0L)

  override def update(updater: Updater): Unit =
    updater match {
      case u: CountUpdaterBase =>
        count.addAndGet(u.count)
    }

  override def result: AnyValue = Values.longValue(count.get)
}
