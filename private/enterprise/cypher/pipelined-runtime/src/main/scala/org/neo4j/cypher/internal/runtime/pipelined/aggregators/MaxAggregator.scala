/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.values.storable.Values
import org.neo4j.values.{AnyValue, AnyValues}


/**
  * Aggregator for max(...).
  */
case object MaxAggregator extends Aggregator {

  override def newUpdater: Updater = new MaxUpdater

  override def newStandardReducer(memoryTracker: QueryMemoryTracker): Reducer = new MaxStandardReducer

  override def newConcurrentReducer: Reducer = new MaxConcurrentReducer

  def shouldUpdate(max: AnyValue, value: AnyValue): Boolean =
    ((max eq Values.NO_VALUE) || AnyValues.COMPARATOR.compare(max, value) < 0) && !(value eq Values.NO_VALUE)

  class MaxUpdater() extends MaxUpdaterBase

  abstract class MaxUpdaterBase extends Updater {
    private[aggregators] var max: AnyValue = Values.NO_VALUE

    override def update(value: AnyValue): Unit =
      if (!(value eq Values.NO_VALUE)) {
        if (MaxAggregator.shouldUpdate(max, value))
          max = value
      }
  }

  class MaxStandardReducer() extends MaxUpdaterBase with Reducer {
    override def update(updater: Updater): Unit =
      updater match {
        case u: MaxUpdater => update(u.max)
      }

    override def result: AnyValue = max
  }

  class MaxConcurrentReducer() extends Reducer {
    private val max = new AtomicReference[AnyValue](Values.NO_VALUE)

    override def update(updater: Updater): Unit =
      updater match {
        case u: MaxUpdater =>
          max.updateAndGet(oldMax => if (MaxAggregator.shouldUpdate(oldMax, u.max)) u.max else oldMax)
      }

    override def result: AnyValue = max.get
  }
}