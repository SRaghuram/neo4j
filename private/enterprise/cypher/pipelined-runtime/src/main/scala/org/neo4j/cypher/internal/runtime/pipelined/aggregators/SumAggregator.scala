/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.exceptions.CypherTypeException
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.ValueMath.overflowSafeAdd

/**
 * Aggregator for sum(...).
 */
case object SumAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new SumStandardReducer
  override def newConcurrentReducer: Reducer = new SumConcurrentReducer

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[SumStandardReducer])

  def failMix() =
    throw new CypherTypeException("sum() cannot mix number and duration")

  def failType(value: AnyValue) =
    throw new CypherTypeException(s"sum() can only handle numerical values, duration, and null. Got $value")
}

/**
 * Aggregator for sum(DISTINCT..).
 */
case object SumDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new DistinctStandardReducer(new SumStandardReducer(), memoryTracker)
  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new SumConcurrentReducer())

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[SumStandardReducer]) +
      HeapEstimator.shallowSizeOfInstance(classOf[DistinctStandardReducer])
}

abstract class SumBase {

  private[aggregators] var seenNumber: Boolean = false
  private[aggregators] var seenDuration: Boolean = false
  private[aggregators] var sumNumber: NumberValue = _
  private[aggregators] var sumDuration: DurationValue = _

  reset()

  protected def reset(): Unit = {
    sumNumber = Values.ZERO_INT
    sumDuration = DurationValue.ZERO
  }

  protected def update(value: AnyValue): Unit = {
    if (value eq Values.NO_VALUE) {
      return
    }
    value match {
      case number: NumberValue =>
        seenNumber = true
        if (!seenDuration) {
          sumNumber = overflowSafeAdd(sumNumber, number)
        } else {
          SumAggregator.failMix()
        }
      case dur: DurationValue =>
        seenDuration = true
        if (!seenNumber) {
          sumDuration = sumDuration.add(dur)
        } else {
          SumAggregator.failMix()
        }
      case _ => SumAggregator.failType(value)
    }
  }
}

class SumStandardReducer() extends SumBase with DirectStandardReducer {

  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue =
    if (seenNumber && seenDuration) {
      SumAggregator.failMix()
    } else if (seenDuration) {
      sumDuration
    } else {
      sumNumber
    }

  // Updater
  override def add(value: AnyValue): Unit = update(value)
}

class SumConcurrentReducer extends Reducer {
  // Note: these volatile flags are set by multiple threads, but only to true, so racing is benign.
  @volatile private[aggregators] var seenNumber = false
  @volatile private[aggregators] var seenDuration = false

  private[aggregators] val sumNumberAR = new AtomicReference[NumberValue](Values.ZERO_INT)
  private[aggregators] val sumDurationAR = new AtomicReference[DurationValue](DurationValue.ZERO)

  private def onNumber(number: NumberValue): Unit = {
    sumNumberAR.updateAndGet(num => overflowSafeAdd(num, number))
    seenNumber = true
  }

  private def onDuration(duration: DurationValue): Unit = {
    sumDurationAR.updateAndGet(dur => dur.add(duration))
    seenDuration = true
  }

  override def newUpdater(): Updater = new Upd()

  override def result: AnyValue =
    if (seenNumber && seenDuration) {
      SumAggregator.failMix()
    } else if (seenDuration) {
      sumDurationAR.get()
    } else {
      sumNumberAR.get()
    }

  class Upd() extends SumBase with Updater {
    override def add(value: AnyValue): Unit = update(value)
    override def applyUpdates(): Unit = {
      if (seenNumber) {
        onNumber(sumNumber)
      } else if (seenDuration) {
        onDuration(sumDuration)
      }
      reset()
    }
  }
}
