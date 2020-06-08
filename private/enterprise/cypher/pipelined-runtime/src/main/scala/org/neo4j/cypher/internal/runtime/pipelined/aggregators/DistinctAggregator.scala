/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentHashMap

import org.eclipse.collections.api.block.procedure.Procedure
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.ScopedMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

abstract class DistinctUpdater() extends Updater {
  protected def seen(e: AnyValue): Boolean

  def forEachSeen(action: Procedure[AnyValue])

  override def update(value: AnyValue): Unit =
    if (!(value eq Values.NO_VALUE)) {
      seen(value)
    }
}

class UnorderedDistinctUpdater(memoryTracker: MemoryTracker) extends DistinctUpdater {
  private val seenSet: DistinctSet[AnyValue] = DistinctSet.createDistinctSet[AnyValue](memoryTracker)

  override protected def seen(e: AnyValue): Boolean =
    seenSet.add(e)

  def forEachSeen(action: Procedure[AnyValue]): Unit = {
    seenSet.each(action)
  }

  override def update(value: AnyValue): Unit =
    if (!(value eq Values.NO_VALUE)) {
      seen(value)
    }

  override def close(): Unit = {
    seenSet.close()
    super.close()
  }
}

class OrderedDistinctUpdater(memoryTracker: MemoryTracker) extends DistinctUpdater {
  private val seenSet: java.util.Set[AnyValue] = new java.util.LinkedHashSet[AnyValue]() // TODO: Use a heap tracking ordered distinct set
  private val scopedMemoryTracker = new ScopedMemoryTracker(memoryTracker)

  override protected def seen(e: AnyValue): Boolean = {
    val added = seenSet.add(e)
    if (added) {
      scopedMemoryTracker.allocateHeap(e.estimatedHeapUsage())
    }
    added
  }

  def forEachSeen(action: Procedure[AnyValue]): Unit = {
    seenSet.forEach(action)
  }

  override def close(): Unit = {
    scopedMemoryTracker.close()
    super.close()
  }
}

trait DistinctInnerReducer {
  def update(value: AnyValue)
  def result: AnyValue
}

abstract class DistinctReducer(inner: DistinctInnerReducer) extends Reducer {
  protected def seen(e: AnyValue): Boolean

  protected def forEachSeen(action: Procedure[AnyValue])

  override def update(updater: Updater): Unit =
    updater match {
      case u: DistinctUpdater =>
        u.forEachSeen(e => {
          if (seen(e)) {
            inner.update(e)
          }
        })
    }

  override def result: AnyValue = inner.result
}

class DistinctStandardReducer(inner: DistinctInnerReducer, memoryTracker: MemoryTracker) extends DistinctReducer(inner) {
  private val seenSet: DistinctSet[AnyValue] = DistinctSet.createDistinctSet(memoryTracker)

  override protected def seen(e: AnyValue): Boolean =
    seenSet.add(e)

  override protected def forEachSeen(action: Procedure[AnyValue]): Unit =
    seenSet.each(action)

  override def close(): Unit = {
    seenSet.close()
    super.close()
  }
}

class DistinctConcurrentReducer(inner: DistinctInnerReducer) extends DistinctReducer(inner) {
  private val seenSet = ConcurrentHashMap.newKeySet[AnyValue]()

  override protected def seen(e: AnyValue): Boolean =
    seenSet.add(e)

  override protected def forEachSeen(action: Procedure[AnyValue]): Unit =
    seenSet.forEach(action)
}
