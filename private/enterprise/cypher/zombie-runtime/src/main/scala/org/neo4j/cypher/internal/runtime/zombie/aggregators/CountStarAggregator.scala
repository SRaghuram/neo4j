/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.aggregators
import java.util.concurrent.atomic.AtomicLong

import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

/**
  * Aggregator for count(*).
  */
case object CountStarAggregator extends Aggregator {

  override def newUpdater: Updater = new CountStarUpdater
  override def newStandardReducer: Reducer = new CountStarStandardReducer
  override def newConcurrentReducer: Reducer = new CountStarConcurrentReducer

  class CountStarUpdater() extends Updater {
    private[CountStarAggregator] var count = 0L
    override def update(value: AnyValue): Unit = count += 1
  }

  class CountStarStandardReducer() extends Reducer {
    private var count = 0L

    override def update(updater: Updater): Unit =
      updater match {
        case u: CountStarUpdater =>
          count += u.count
      }

    override def result: AnyValue = Values.longValue(count)
  }

  class CountStarConcurrentReducer() extends Reducer {
    private val count = new AtomicLong(0L)

    override def update(updater: Updater): Unit =
      updater match {
        case u: CountStarUpdater =>
          count.addAndGet(u.count)
      }

    override def result: AnyValue = Values.longValue(count.get)
  }
}
