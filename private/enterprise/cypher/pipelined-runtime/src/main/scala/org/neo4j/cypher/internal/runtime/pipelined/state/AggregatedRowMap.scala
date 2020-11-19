/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util
import java.util.Map
import java.util.concurrent.ConcurrentHashMap

import org.eclipse.collections.api.block.function.Function2
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Reducer
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StandardReducer
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Updater
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

// ============ Interface ============

/**
 * The [[AggregatedRow]] builds and outputs all the aggregates for one aggregation output row.
 */
trait AggregatedRow {
  def updaters(workerId: Int): AggregatedRowUpdaters
  def result(index: Int): AnyValue
}

/**
 * The [[AggregatedRowUpdaters]] holds all the aggregators for one aggregation output row.
 */
trait AggregatedRowUpdaters {
  def initialize(n: Int, state: QueryState): Unit
  def updater(index: Int): Updater
  def addUpdate(index: Int, value: AnyValue): Unit
  def addUpdate(index: Int, value: Array[AnyValue]): Unit
  def applyUpdates(): Unit
}

/**
 * Maps grouping values to [[AggregatedRow]]s.
 */
trait AggregatedRowMap extends MorselAccumulator[AnyRef] {

  def get(groupingValue: AnyValue): AggregatedRow
  def applyUpdates(workedId: Int): Unit

  def argumentRow: MorselRow
  def result(): java.util.Iterator[java.util.Map.Entry[AnyValue, AggregatedRow]]

  override def update(data: AnyRef, resources: QueryResources): Unit = {}
}

// ============ Factory ============

object AggregatedRowMap {
  class Factory(aggregators: Array[Aggregator], numberOfWorkers: Int) extends ArgumentStateFactory[AggregatedRowMap] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): AggregatedRowMap =
      new StandardAggregationMap(argumentRowId, aggregators, argumentRowIdsForReducers, argumentMorsel.snapshot(), memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): AggregatedRowMap =
      new ConcurrentAggregationMap(argumentRowId, aggregators, argumentRowIdsForReducers, argumentMorsel.snapshot(), numberOfWorkers)
  }
}

// ============ Standard ============

class StandardAggregationMap(override val argumentRowId: Long,
                             aggregators: Array[Aggregator],
                             override val argumentRowIdsForReducers: Array[Long],
                             override val argumentRow: MorselRow,
                             memoryTracker: MemoryTracker) extends AggregatedRowMap {
  private[this] val reducerMap: HeapTrackingOrderedAppendMap[AnyValue, StandardAggregators] =
    HeapTrackingOrderedAppendMap.createOrderedMap[AnyValue, StandardAggregators](memoryTracker)
  private[this] val estimatedShallowSizeOfMapValue =
    StandardAggregators.SHALLOW_SIZE +
    HeapEstimator.shallowSizeOfObjectArray(aggregators.length) +
    aggregators.map(_.standardShallowSize).sum

  private[this] val needToApplyUpdates = !Aggregator.allDirect(aggregators)

  private[this] val newAggregators: Function2[AnyValue, MemoryTracker, StandardAggregators] =
    (groupingValue: AnyValue, scopedMemoryTracker: MemoryTracker) => {
      val aggLength = aggregators.length
      scopedMemoryTracker.allocateHeap(groupingValue.estimatedHeapUsage() + estimatedShallowSizeOfMapValue)
      val reducers = new Array[StandardReducer](aggLength)
      var i = 0
      while (i < aggLength) {
        reducers(i) = aggregators(i).newStandardReducer(scopedMemoryTracker)
        i += 1
      }
      new StandardAggregators(reducers)
    }

  override def get(groupingValue: AnyValue): AggregatedRow =
    reducerMap.getIfAbsentPutWithMemoryTracker2(groupingValue, newAggregators)

  override def applyUpdates(workerId: Int): Unit =
    if (needToApplyUpdates) {
      reducerMap.forEachValue(_.applyUpdates())
    }

  override def result(): java.util.Iterator[java.util.Map.Entry[AnyValue, AggregatedRow]] =
    reducerMap.autoClosingEntryIterator().asInstanceOf[java.util.Iterator[java.util.Map.Entry[AnyValue, AggregatedRow]]]

  override def close(): Unit = {
    reducerMap.close()
    super.close()
  }

  override final def shallowSize: Long = StandardAggregationMap.SHALLOW_SIZE
}

object StandardAggregationMap {
  private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[StandardAggregationMap])
}

object StandardAggregators {
  val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[StandardAggregators])
}

class StandardAggregators(reducers: Array[StandardReducer]) extends AggregatedRow with AggregatedRowUpdaters {

  // Aggregators
  override def updaters(workerId: Int): AggregatedRowUpdaters = this
  override def result(n: Int): AnyValue = reducers(n).result

  // Update

  override def initialize(n: Int, state: QueryState): Unit = reducers(n).initialize(state)

  override def updater(n: Int): Updater = reducers(n)
  override def addUpdate(n: Int, value: AnyValue): Unit = reducers(n).add(value)
  override def addUpdate(n: Int, value: Array[AnyValue]): Unit = reducers(n).add(value)
  override def applyUpdates(): Unit = {
    var i = 0
    while (i < reducers.length) {
      reducers(i).applyUpdates()
      i += 1
    }
  }
}

class StandardDirectAggregators(reducers: Array[StandardReducer]) extends AggregatedRow with AggregatedRowUpdaters {

  // Aggregators
  override def updaters(workerId: Int): AggregatedRowUpdaters = this
  override def result(n: Int): AnyValue = reducers(n).result

  // Update
  override def initialize(n: Int, state: QueryState): Unit = reducers(n).initialize(state)
  override def updater(n: Int): Updater = reducers(n)
  override def addUpdate(n: Int, value: AnyValue): Unit = reducers(n).add(value)
  override def addUpdate(n: Int, value: Array[AnyValue]): Unit = reducers(n).add(value)
  override def applyUpdates(): Unit = {} // Noop, since updates to direct aggregators are applied directly on addUpdate
}

// ============ Concurrent ============

class ConcurrentAggregationMap(override val argumentRowId: Long,
                               aggregators: Array[Aggregator],
                               override val argumentRowIdsForReducers: Array[Long],
                               override val argumentRow: MorselRow,
                               numberOfWorkers: Int) extends AggregatedRowMap {

  val reducerMap = new ConcurrentHashMap[AnyValue, ConcurrentAggregators]

  override def get(groupingValue: AnyValue): AggregatedRow = reducerMap.computeIfAbsent(groupingValue, newAggregators)
  override def applyUpdates(workerId: Int): Unit = {
    val values = reducerMap.values().iterator()
    while (values.hasNext) {
      values.next().applyUpdates(workerId)
    }
  }
  override def result(): util.Iterator[Map.Entry[AnyValue, AggregatedRow]] = reducerMap.entrySet().iterator().asInstanceOf[java.util.Iterator[java.util.Map.Entry[AnyValue, AggregatedRow]]]

  private def newAggregators(groupingValue: AnyValue): ConcurrentAggregators = {
    val reducers = new Array[Reducer](aggregators.length)
    var i = 0
    while (i < aggregators.length) {
      reducers(i) = aggregators(i).newConcurrentReducer
      i += 1
    }
    new ConcurrentAggregators(reducers, numberOfWorkers)
  }

  override final def shallowSize: Long = ConcurrentAggregationMap.SHALLOW_SIZE
}

object ConcurrentAggregationMap {
  private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[ConcurrentAggregationMap])
}

class ConcurrentAggregators(reducers: Array[Reducer], numberOfWorkers: Int) extends AggregatedRow {

  private val _updaters = (0 until numberOfWorkers).map(_ => new ConcurrentUpdaters(reducers.map(_.newUpdater()))).toArray

  override def updaters(workerId: Int): AggregatedRowUpdaters = _updaters(workerId)
  override def result(n: Int): AnyValue = reducers(n).result
  def applyUpdates(workerId: Int): Unit = {
    _updaters(workerId).applyUpdates()
  }
}

class ConcurrentUpdaters(updaters: Array[Updater]) extends AggregatedRowUpdaters {
  override def updater(n: Int): Updater = updaters(n)
  override def addUpdate(n: Int, value: AnyValue): Unit = updaters(n).add(value)
  override def addUpdate(n: Int, value: Array[AnyValue]): Unit = updaters(n).add(value)
  def applyUpdates(): Unit = {
    updaters.foreach(_.applyUpdates())
  }

  override def initialize(n: Int, state: QueryState): Unit = updaters(n).initialize(state)
}
