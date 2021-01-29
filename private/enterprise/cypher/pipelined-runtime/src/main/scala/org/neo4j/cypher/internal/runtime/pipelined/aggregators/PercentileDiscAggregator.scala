/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util
import java.util.Collections

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
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

/**
 * Aggregator for percentileDisc(DISTINCT...).
 */
case object PercentileDiscDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new DistinctStandardReducer(new PercentileDiscStandardReducer(memoryTracker), memoryTracker)
  override def newConcurrentReducer: Reducer = new DistinctConcurrentMultiArgumentReducer(new PercentileDiscConcurrentReducer(), 0)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[PercentileDiscStandardReducer])
}

/**
 * Aggregator for percentileCont(...).
 */
case object PercentileContAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new PercentileContStandardReducer(memoryTracker)
  override def newConcurrentReducer: Reducer = new PercentileContConcurrentReducer()

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[PercentileContStandardReducer])
}

/**
 * Aggregator for percentileCont(DISTINCT...).
 */
case object PercentileContDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new DistinctStandardReducer(new PercentileContStandardReducer(memoryTracker), memoryTracker)
  override def newConcurrentReducer: Reducer = new DistinctConcurrentMultiArgumentReducer(new PercentileContConcurrentReducer(), 0)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[PercentileDiscStandardReducer])
}

abstract class PercentileStandardReducer(memoryTracker: MemoryTracker) extends DirectStandardReducer {
  protected var estimatedNumberSize: Long = -1L
  protected val tmp: HeapTrackingArrayList[NumberValue] = HeapTrackingCollections.newArrayList[NumberValue](memoryTracker)
  protected var percent: Double = -1
  // Reducer
  override def newUpdater(): Updater = this

  // Updater
  override def add(value: AnyValue): Unit = throw new IllegalStateException("Percentile functions takes two arguments")
  override def add(values: Array[AnyValue]): Unit = {
    val value = values(0)
    if (value eq Values.NO_VALUE) {
      return
    }
    val number = CypherCoercions.asNumberValue(value)
    if (percent < 0) {
      percent = CypherCoercions.asNumberValue(values(1)).doubleValue()
      if (percent < 0 || percent > 1.0) {
        throw new InvalidArgumentException(
          s"Invalid input '$percent' is not a valid argument, must be a number in the range 0.0 to 1.0")
      }
    }
    tmp.add(number)
    if(estimatedNumberSize == -1L) {
      estimatedNumberSize = number.estimatedHeapUsage()
    }
    memoryTracker.allocateHeap(estimatedNumberSize)
  }
}

class PercentileContStandardReducer(memoryTracker: MemoryTracker) extends PercentileStandardReducer(memoryTracker) {
  override def result(state: QueryState): AnyValue = {
    tmp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))
    val count = tmp.size
    val r = if (percent == 1.0 || count == 1) {
      tmp.get(count - 1)
    } else if (count > 1) {
      val floatIdx = percent * (count - 1)
      val floor = floatIdx.toInt
      val ceil = math.ceil(floatIdx).toInt
      if (ceil == floor || floor == count - 1) tmp.get(floor)
      else tmp.get(floor).times(ceil - floatIdx).plus(tmp.get(ceil).times(floatIdx - floor))
    } else {
      Values.NO_VALUE
    }

    memoryTracker.releaseHeap(count*estimatedNumberSize)
    tmp.close()
    r
  }
}

class PercentileDiscStandardReducer(memoryTracker: MemoryTracker) extends PercentileStandardReducer(memoryTracker) {
  override def result(state: QueryState): AnyValue = {
    tmp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))
    val count = tmp.size
    val r = if (percent == 1.0 || count == 1) {
      tmp.get(count - 1)
    } else if (count > 1) {
      val floatIdx = percent * count
      val toInt = floatIdx.toInt
      val idx = if (floatIdx != toInt || toInt == 0) toInt else toInt - 1
      tmp.get(idx)
    } else {
      Values.NO_VALUE
    }

    memoryTracker.releaseHeap(count*estimatedNumberSize)
    tmp.close()
    r
  }
}

abstract class PercentileConcurrentReducer() extends Reducer {
  @volatile protected var percent: Double = -1
  protected val tmp: util.List[NumberValue] = Collections.synchronizedList(new util.ArrayList[NumberValue])

  override def newUpdater(): Updater = new Upd
  class Upd() extends Updater {
    private val localCollection = new util.ArrayList[NumberValue]
    override def add(values: Array[AnyValue]): Unit = {
      val value = values(0)
      if (value eq Values.NO_VALUE) {
        return
      }
      val number = CypherCoercions.asNumberValue(value)
      if (percent < 0) {
        val p = CypherCoercions.asNumberValue(values(1)).doubleValue()
        if (p < 0 || p > 1.0) {
          throw new InvalidArgumentException(
            s"Invalid input '$percent' is not a valid argument, must be a number in the range 0.0 to 1.0")
        }
        percent = p
      }
      localCollection.add(number)
    }

    override def applyUpdates(): Unit = {
      tmp.addAll(localCollection)
      localCollection.clear()
    }

    override def add(value: AnyValue): Unit = throw new IllegalStateException("Percentile functions takes two arguments")
  }
}

class PercentileContConcurrentReducer() extends PercentileConcurrentReducer {
  override def result(state: QueryState): AnyValue = {
    tmp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))

    val count = tmp.size()
    //volatile read, just do it once
    val perc = percent
    if (perc == 1.0 || count == 1) {
      tmp.get(count - 1)
    } else if (count > 1) {
      val c = count
      val floatIdx = perc * (c - 1)
      val floor = floatIdx.toInt
      val ceil = math.ceil(floatIdx).toInt
      if (ceil == floor || floor == c - 1) tmp.get(floor)
      else tmp.get(floor).times(ceil - floatIdx).plus(tmp.get(ceil).times(floatIdx - floor))
    } else {
      Values.NO_VALUE
    }
  }
}

class PercentileDiscConcurrentReducer() extends PercentileConcurrentReducer {
  override def result(state: QueryState): AnyValue = {
    tmp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))
    val count = tmp.size()
    //volatile read, just do it once
    val perc = percent
    if (perc == 1.0 || count == 1) {
      tmp.get(count - 1)
    } else if (count > 1) {
      val floatIdx = perc * count
      val toInt = floatIdx.toInt
      val idx = if (floatIdx != toInt || toInt == 0) toInt else toInt - 1
      tmp.get(idx)
    } else {
      Values.NO_VALUE
    }
  }
}