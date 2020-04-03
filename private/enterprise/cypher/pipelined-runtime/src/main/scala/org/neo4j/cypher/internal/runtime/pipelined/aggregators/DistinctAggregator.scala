/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

class DistinctUpdater() extends Updater {
  val seen: java.util.Set[AnyValue] = new java.util.HashSet[AnyValue]()
  override def update(value: AnyValue): Unit =
    if (!(value eq Values.NO_VALUE)) {
      seen.add(value)
    }
}

trait DistinctInnerReducer {
  def update(value: AnyValue)
  def result: AnyValue
}

abstract class DistinctReducer(inner: DistinctInnerReducer) extends Reducer {
  protected val seen: java.util.Set[AnyValue]

  override def update(updater: Updater): Unit =
    updater match {
      case u: DistinctUpdater =>
        u.seen.forEach(e => {
          if (seen.add(e)) {
            inner.update(e)
          }
        })
    }

  override def result: AnyValue = inner.result
}

class DistinctStandardReducer(inner: DistinctInnerReducer) extends DistinctReducer(inner) {
  override protected val seen: java.util.Set[AnyValue] = new java.util.HashSet[AnyValue]()
}

class DistinctConcurrentReducer(inner: DistinctInnerReducer) extends DistinctReducer(inner) {
  override protected val seen: java.util.Set[AnyValue] = ConcurrentHashMap.newKeySet()
}
