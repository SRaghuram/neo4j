

/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.exceptions.CypherTypeException
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

case object StdevAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new StdevStandardReducer(false)

  override def newConcurrentReducer: Reducer = new StdevConcurrentReducer(false)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[StdevStandardReducer])

  def failType(value: AnyValue) =
    throw new CypherTypeException(s"stdev()/stdevp() can only handle numerical values, and null. Got $value")
}

case object StdevDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer =
    new DistinctStandardReducer(new StdevStandardReducer(false), memoryTracker)

  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new StdevConcurrentReducer(false))

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[StdevStandardReducer]) +
      HeapEstimator.shallowSizeOfInstance(classOf[DistinctStandardReducer])
}
case object StdevPAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new StdevStandardReducer(true)

  override def newConcurrentReducer: Reducer = new StdevConcurrentReducer(true)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[StdevStandardReducer])
}

case object StdevPDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer =
    new DistinctStandardReducer(new StdevStandardReducer(true), memoryTracker)

  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new StdevConcurrentReducer(true))

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[StdevStandardReducer]) +
      HeapEstimator.shallowSizeOfInstance(classOf[DistinctStandardReducer])
}

/**
 * Taking inspiration from:
 *
 * http://www.alias-i.com/lingpipe/src/com/aliasi/stats/OnlineNormalEstimator.java
 * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
 */
abstract class StdevBase() {
  private[aggregators] var count: Long = _
  private[aggregators] var movingAvg: Double = _
  // sum of squares of differences from the current mean
  private[aggregators] var m2: Double = _

  reset()

  protected def reset(): Unit = {
    count = 0L
    movingAvg = 0.0
    m2 = 0.0
  }

  protected def update(value: AnyValue): Unit = {
    if (value eq Values.NO_VALUE) {
      return
    }
    count += 1

    value match {
      case number: NumberValue =>
        val x = number.doubleValue()
        val nextM = movingAvg + (x - movingAvg) / count
        m2 += (x - movingAvg) * (x - nextM)
        movingAvg = nextM
      case _ => StdevAggregator.failType(value)
    }
  }
}

class StdevStandardReducer(val population: Boolean) extends StdevBase with StandardReducer with DirectStandardReducer {

  override def newUpdater(): Updater = this

  override def result: AnyValue =
    if (count < 2) {
      Values.ZERO_FLOAT
    } else {
      val variance = if (population) m2 / count else m2 / (count - 1)
      Values.doubleValue(math.sqrt(variance))
    }

  // Updater
  override def add(value: Array[AnyValue]): Unit = update(value(0))
}

// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
class StdevConcurrentReducer(val population: Boolean) extends Reducer {

  case class StdevNumber(count: Long = 0L,
                         avg: Double = 0.0,
                         m2: Double = 0.0)

  private val runningStdevNumber = new AtomicReference[StdevNumber](StdevNumber())

  override def newUpdater(): Updater = new Upd()

  override def result: AnyValue = {
    val x = runningStdevNumber.get()

    if (x.count < 2) {
      Values.ZERO_FLOAT
    } else {
      val variance = if (population) x.m2 / x.count else x.m2 / (x.count - 1)
      Values.doubleValue(math.sqrt(variance))
    }
  }

  private def onNumberUpdate(partAvg: Double, partM2: Double, partCount: Long): Unit = {
    runningStdevNumber.updateAndGet(old => {
      val newCount = old.count + partCount
      val delta = partAvg - old.avg
      val nextAvg = old.avg + delta * partCount.toDouble / newCount
      val nextM2 = old.m2 + partM2 + delta * delta * old.count * partCount / newCount

      StdevNumber(newCount, nextAvg, nextM2)
    })
  }

  class Upd() extends StdevBase with Updater {

    override def add(value: Array[AnyValue]): Unit = update(value(0))

    override def applyUpdates(): Unit = {
      onNumberUpdate(movingAvg, m2, count)
      reset()
    }
  }
}
