/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.DistinctConcurrentMultiArgumentReducer.InputRow
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.Measurable
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

class DistinctStandardReducer(inner: StandardReducer, memoryTracker: MemoryTracker, distinctColumn: Int = 0) extends StandardReducer {
  // NOTE: The owner is responsible for closing the given memory tracker in the right scope, so we do not need to close the seenSet explicitly
  private val seenSet: DistinctSet[AnyValue] = DistinctSet.createDistinctSet(memoryTracker)

  // Reducer
  override def newUpdater(): Updater = this
  override def result(state: QueryState): AnyValue = inner.result(state)

  // Updater
  override def add(value: AnyValue): Unit = {
    if (seenSet.add(value)) {
      inner.add(value)
    }
  }

  /**
   * Note we always assume the distinct is on the first column
   */
  override def add(values: Array[AnyValue]): Unit = {
    if (seenSet.add(values(distinctColumn))) {
      inner.add(values)
    }
  }

  override def isDirect: Boolean = inner.isDirect
  override def applyUpdates(): Unit = inner.applyUpdates()
}

class DistinctConcurrentReducer(inner: Reducer) extends Reducer {
  private val seenSet = ConcurrentHashMap.newKeySet[AnyValue]()

  override def newUpdater(): Updater = new Upd(inner.newUpdater())
  override def result(state: QueryState): AnyValue = inner.result(state)

  class Upd(inner: Updater) extends Updater {
    private var partSeenSet: DistinctSet[AnyValue] = DistinctSet.createDistinctSet[AnyValue](EmptyMemoryTracker.INSTANCE)

    override def add(value: AnyValue): Unit = partSeenSet.add(value)
    override def applyUpdates(): Unit = {
      partSeenSet.each(x => {
        if (seenSet.add(x)) {
          inner.add(x)
        }
      })
      partSeenSet = DistinctSet.createDistinctSet[AnyValue](EmptyMemoryTracker.INSTANCE)
      inner.applyUpdates()
    }
  }
}

class DistinctConcurrentMultiArgumentReducer(inner: Reducer, distinctIndex: Int) extends Reducer {

  private val seenSet = ConcurrentHashMap.newKeySet[InputRow]()

  override def newUpdater(): Updater = new Upd(inner.newUpdater())
  override def result(state: QueryState): AnyValue = inner.result(state)

  class Upd(inner: Updater) extends Updater {
    private var partSeenSet: DistinctSet[InputRow] = DistinctSet.createDistinctSet[InputRow](EmptyMemoryTracker.INSTANCE)

    override def add(value: AnyValue): Unit = throw new IllegalStateException("should use add(values: Array[AnyValue])")
    override def add(values: Array[AnyValue]): Unit = partSeenSet.add(new InputRow(values, distinctIndex))
    override def applyUpdates(): Unit = {
      partSeenSet.each(x => {
        if (seenSet.add(x)) {
          inner.add(x.inputs)
        }
      })
      partSeenSet = DistinctSet.createDistinctSet[InputRow](EmptyMemoryTracker.INSTANCE)
      inner.applyUpdates()
    }
  }
}

object DistinctConcurrentMultiArgumentReducer {
  class InputRow(val inputs: Array[AnyValue], val index: Int) extends Measurable {
    override def hashCode(): Int = inputs(index).hashCode()
    override def equals(obj: Any): Boolean = obj match {
      case other: InputRow => inputs.length == other.inputs.length && index == other.index && inputs(index) == other.inputs(index)
      case _ => false
    }

    override def estimatedHeapUsage(): Long = 0L//we are not tracking memory in the concurrent case
  }
}
