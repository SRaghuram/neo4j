/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.lang.Double.doubleToLongBits
import java.lang.Double.longBitsToDouble
import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.operations.CypherCoercions
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

/**
 * Aggregator for percentileDisc(...).
 */
case object PercentileDiscAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new PercentileDiscStandardReducer(memoryTracker)
  override def newConcurrentReducer: Reducer = new PercentileDiscConcurrentReducer()

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[PercentileDiscStandardReducer])
}

class PercentileDiscStandardReducer(memoryTracker: MemoryTracker) extends DirectStandardReducer {
  private val tmp = HeapTrackingCollections.newArrayList[NumberValue](memoryTracker)
  private var count: Int = 0
  private var percent: Double = 0.0
  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue = {
    tmp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))
        if (percent == 1.0 || count == 1) {
          tmp.get(tmp.size() - 1)
        } else if (count > 1) {
          val floatIdx = percent * count
          var idx = floatIdx.toInt
          idx = if (floatIdx != idx || idx == 0) idx
          else idx - 1
          tmp.get(idx)
        } else {
          Values.NO_VALUE
        }
  }

  // Updater
  override def add(values: Array[AnyValue]): Unit = {
    val value = values(0)
    if (value eq Values.NO_VALUE) {
      return
    }
    val number = CypherCoercions.asNumberValue(value)
    percent = CypherCoercions.asNumberValue(values(1)).doubleValue()
    if (percent < 0 || percent > 1.0) {
      throw new InvalidArgumentException(
        s"Invalid input '$percent' is not a valid argument, must be a number in the range 0.0 to 1.0")
    }
    count += 1
    tmp.add(number)
  }
}

class PercentileDiscConcurrentReducer() extends Reducer {
  private val count = new AtomicLong(0L)
  private val percent = new AtomicLong(0L)
  private val tmp = Collections.synchronizedList(new util.ArrayList[NumberValue])

  override def newUpdater(): Updater = new Upd
  override def result: AnyValue = {
    tmp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))

    val perc = longBitsToDouble(percent.get())
    if (perc == 1.0 || count.get() == 1) {
      tmp.get(tmp.size() - 1)
    } else if (count.get() > 1) {
      val floatIdx = perc * count.get()
      var idx = floatIdx.toInt
      idx = if (floatIdx != idx || idx == 0) idx
      else idx - 1
      tmp.get(idx)
    } else {
      Values.NO_VALUE
    }
  }

  class Upd() extends Updater {
    private val localCollection = new util.ArrayList[NumberValue]
    private var localCount = 0
    private var localPercent = 0.0
    override def add(values: Array[AnyValue]): Unit = {
      val value = values(0)
      if (value eq Values.NO_VALUE) {
        return
      }
      val number = CypherCoercions.asNumberValue(value)
      val p = CypherCoercions.asNumberValue(values(1)).doubleValue()
      if (p < 0 || p > 1.0) {
        throw new InvalidArgumentException(
          s"Invalid input '$percent' is not a valid argument, must be a number in the range 0.0 to 1.0")
      }
      localPercent = p
      localCount += 1
      localCollection.add(number)
    }


    override def applyUpdates(): Unit = {
      percent.set(doubleToLongBits(localPercent))
      count.addAndGet(localCount)
      tmp.addAll(localCollection)
    }
  }
}
