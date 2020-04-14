/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.exceptions.CypherTypeException
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
  override def newUpdater: Updater = new SumUpdater
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new SumStandardReducer
  override def newConcurrentReducer: Reducer = new SumConcurrentReducer
}

/**
 * Aggregator for sum(DISTINCT..).
 */
case object SumDistinctAggregator extends Aggregator {
  override def newUpdater: Updater = new DistinctUpdater
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new DistinctStandardReducer(new SumDistinctStandardReducer())
  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new SumDistinctConcurrentReducer())
}

trait SumStandardBase {

  private[aggregators] var seenNumber = false
  private[aggregators] var seenDuration = false
  private[aggregators] var sumNumber: NumberValue = Values.ZERO_INT
  private[aggregators] var sumDuration: DurationValue = DurationValue.ZERO

  protected def onNumber(number: NumberValue): Unit = {
    seenNumber = true
    if (!seenDuration) {
      sumNumber = overflowSafeAdd(sumNumber, number)
    } else {
      failMix()
    }
  }

  protected def onDuration(dur: DurationValue): Unit = {
    seenDuration = true
    if (!seenNumber) {
      sumDuration = sumDuration.add(dur)
    } else {
      failMix()
    }
  }

  protected def failMix() =
    throw new CypherTypeException("sum() cannot mix number and duration")

  protected def failType(value: AnyValue) =
    throw new CypherTypeException(s"sum() can only handle numerical values, duration, and null. Got $value")
}

class SumUpdater() extends SumStandardBase with Updater {

  override def update(value: AnyValue): Unit = {
    if (value eq Values.NO_VALUE) {
      return
    }
    value match {
      case number: NumberValue => onNumber(number)
      case dur: DurationValue => onDuration(dur)
      case _ => failType(value)
    }
  }
}

trait SumResultProvider {
  private[aggregators] def seenNumber: Boolean
  private[aggregators] def seenDuration: Boolean
  private[aggregators] def sumDuration: DurationValue
  private[aggregators] def sumNumber: NumberValue

  def result: AnyValue =
    if (seenNumber && seenDuration) {
      throw new CypherTypeException("sum() cannot mix number and duration")
    } else if (seenDuration) {
      sumDuration
    } else {
      sumNumber
    }
}

class SumStandardReducer() extends SumResultProvider with SumStandardBase with Reducer {

  override def update(updater: Updater): Unit =
    updater match {
      case u: SumUpdater =>
        if (u.seenNumber) {
          onNumber(u.sumNumber)
        } else if (u.seenDuration) onDuration(u.sumDuration)
    }
}

trait SumConcurrentReducerBase extends SumResultProvider {
  // Note: these volatile flags are set by multiple threads, but only to true, so racing is benign.
  @volatile private[aggregators] var seenNumber = false
  @volatile private[aggregators] var seenDuration = false

  private[aggregators] val sumNumberAR = new AtomicReference[NumberValue](Values.ZERO_INT)
  private[aggregators] val sumDurationAR = new AtomicReference[DurationValue](DurationValue.ZERO)

  override private[aggregators] def sumDuration = sumDurationAR.get()
  override private[aggregators] def sumNumber = sumNumberAR.get()

  def onNumber(number: NumberValue): Unit = {
    sumNumberAR.updateAndGet(num => overflowSafeAdd(num, number))
    seenNumber = true
  }

  def onDuration(duration: DurationValue): Unit = {
    sumDurationAR.updateAndGet(dur => dur.add(duration))
    seenDuration = true
  }
}

class SumConcurrentReducer() extends SumConcurrentReducerBase with Reducer {
  override def update(updater: Updater): Unit =
    updater match {
      case u: SumUpdater =>
        if (u.seenNumber) {
          onNumber(u.sumNumber)
        } else if (u.seenDuration) {
          onDuration(u.sumDuration)
        }
    }
}

class SumDistinctStandardReducer() extends SumUpdater with DistinctInnerReducer with SumResultProvider

class SumDistinctConcurrentReducer() extends SumConcurrentReducerBase with DistinctInnerReducer {
  override def update(value: AnyValue): Unit = value match {
    case number: NumberValue => onNumber(number)
    case duration: DurationValue => onDuration(duration)
  }
}
