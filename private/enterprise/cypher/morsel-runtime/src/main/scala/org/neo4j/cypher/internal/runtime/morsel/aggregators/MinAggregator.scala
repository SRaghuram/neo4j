/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.aggregators

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.values.storable.Values
import org.neo4j.values.{AnyValue, AnyValues}


/**
  * Aggregator for min(...).
  */
case object MinAggregator extends Aggregator {

  override def newUpdater: Updater = new MinUpdater
  override def newStandardReducer: Reducer = new MinStandardReducer
  override def newConcurrentReducer: Reducer = new MinConcurrentReducer

  def shouldUpdate(min: AnyValue, value: AnyValue): Boolean =
    (min eq Values.NO_VALUE) || AnyValues.COMPARATOR.compare(min, value) > 0
}

class MinUpdater() extends MinUpdaterBase
abstract class MinUpdaterBase extends Updater {
  private[aggregators] var min: AnyValue = Values.NO_VALUE
  override def update(value: AnyValue): Unit =
    if (!(value eq Values.NO_VALUE)) {
      if (MinAggregator.shouldUpdate(min, value))
        min = value
    }
}

class MinStandardReducer() extends MinUpdaterBase with Reducer {
  override def update(updater: Updater): Unit =
    updater match {
      case u: MinUpdater => update(u.min)
    }

  override def result: AnyValue = min
}

class MinConcurrentReducer() extends Reducer {
  private val min = new AtomicReference[AnyValue](Values.NO_VALUE)

  override def update(updater: Updater): Unit =
    updater match {
      case u: MinUpdater =>
        min.updateAndGet(oldMin => if (MinAggregator.shouldUpdate(oldMin, u.min)) u.min else oldMin)
    }

  override def result: AnyValue = min.get
}
