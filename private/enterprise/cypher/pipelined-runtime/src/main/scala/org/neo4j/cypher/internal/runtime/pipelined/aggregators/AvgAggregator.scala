/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.time.temporal.ChronoUnit
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
 * Aggregator for avg(...).
 *
 * To make this work efficiently we had to generalize the cumulative moving average approach
 * to work with batched updates in the reducer. The algebraic derivations used are listed below,
 * for the simple value at a time formulae, see https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average.
 *
 *   n: cumulative count
 *   CMA: cumulative moving average
 *
 *   m: updated count
 *   UA: updater average
 *   US: updater sum
 *
 *     # this is the standard algebraic way to compute a joint average from two precomputed averages.
 *   CMA = (m * UA +          n  * CMA) / (n + m)
 *
 *     # analogous to CMA
 *   CMA = (m * UA + (n + m - m) * CMA) / (n + m)
 *   CMA = ((n + m) * CMA + m * UA - (m * CMA)) / (n + m)
 *   CMA =  CMA +          (m * UA - (m * CMA)) / (n + m)
 *
 *    # for numbers we derive an overflow-safe formulation using batch averages
 *   CMA =  CMA +          (m * (UA - CMA)) / (n + m)
 *   CMA =  CMA +          ((UA - CMA) * m) / (n + m)
 *   CMA =  CMA +           (UA - CMA) * (m / (n + m))
 *
 *    # for durations we derive a faster formulation using batch sums, because we won't overflow
 *   CMA =  CMA +              (US - (m * CMA)) / n + m
 */
case object AvgAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new AvgStandardReducer
  override def newConcurrentReducer: Reducer = new AvgConcurrentReducer

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[AvgStandardReducer])

  def failMix() =
    throw new CypherTypeException("avg() cannot mix number and duration")

  def failType(value: AnyValue) =
    throw new CypherTypeException(s"avg() can only handle numerical values, duration, and null. Got $value")
}

case object AvgDistinctAggregator extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new DistinctStandardReducer(new AvgStandardReducer, memoryTracker)
  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new AvgConcurrentReducer)

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[AvgStandardReducer]) +
      HeapEstimator.shallowSizeOfInstance(classOf[DistinctStandardReducer])
}

abstract class AvgBase() {

  private[aggregators] var seenNumber: Boolean = false
  private[aggregators] var seenDuration: Boolean = false

  private[aggregators] var partCount: Long = _

  private[aggregators] var avgNumber: NumberValue = _

  private[aggregators] var sumMonths: Long = _
  private[aggregators] var sumDays: Long = _
  private[aggregators] var sumSeconds: Long = _
  private[aggregators] var sumNanos: Long = _

  reset()

  protected def reset(): Unit = {
    partCount = 0L
    avgNumber = Values.ZERO_INT
    sumMonths = 0L
    sumDays = 0L
    sumSeconds = 0L
    sumNanos = 0L
  }

  protected def update(value: AnyValue): Unit = {
    if (value eq Values.NO_VALUE) {
      return
    }
    partCount += 1

    value match {
      case number: NumberValue =>
        seenNumber = true
        if (!seenDuration) {
          val diff = number.minus(avgNumber)
          val next = diff.dividedBy(partCount.toDouble)
          avgNumber = overflowSafeAdd(avgNumber, next)
        } else {
          AvgAggregator.failMix()
        }

      case dur: DurationValue =>
        seenDuration = true
        if (!seenNumber) {
          sumMonths += dur.get(ChronoUnit.MONTHS)
          sumDays += dur.get(ChronoUnit.DAYS)
          sumSeconds += dur.get(ChronoUnit.SECONDS)
          sumNanos += dur.get(ChronoUnit.NANOS)
        } else {
          AvgAggregator.failMix()
        }

      case _ => AvgAggregator.failType(value)
    }
  }
}

class AvgStandardReducer() extends AvgBase with StandardReducer {

  protected var count: Long = 0L

  protected var monthsRunningAvg = 0d
  protected var daysRunningAvg = 0d
  protected var secondsRunningAvg = 0d
  protected var nanosRunningAvg = 0d

  // Reducer
  override def newUpdater(): Updater = this
  override def result: AnyValue =
    if (seenDuration) {
      DurationValue.approximate(monthsRunningAvg, daysRunningAvg, secondsRunningAvg, nanosRunningAvg).normalize()
    } else if (seenNumber) {
      avgNumber
    } else {
      Values.NO_VALUE
    }

  // Updater
  override def add(value: Array[AnyValue]): Unit = update(value(0))
  override def applyUpdates(): Unit = {
    // do nothing if number
    if (seenDuration) {
      val newCount = partCount + count
      monthsRunningAvg += (sumMonths - monthsRunningAvg * partCount) / newCount
      daysRunningAvg += (sumDays - daysRunningAvg * partCount) / newCount
      secondsRunningAvg += (sumSeconds - secondsRunningAvg * partCount) / newCount
      nanosRunningAvg += (sumNanos - nanosRunningAvg * partCount) / newCount
      count = newCount
      reset()
    }
  }
}

class AvgConcurrentReducer() extends Reducer {

  // Note: these volatile flags are set by multiple threads, but only to true, so racing is benign.
  @volatile private var seenNumber = false
  @volatile private var seenDuration = false

  case class AvgDuration(count: Long = 0L,
                         monthsRunningAvg: Double = 0d,
                         daysRunningAvg: Double = 0d,
                         secondsRunningAvg: Double = 0d,
                         nanosRunningAvg: Double = 0d)

  private val durationRunningAvg = new AtomicReference[AvgDuration](AvgDuration())

  case class AvgNumber(count: Long = 0L,
                       avgNumber: NumberValue = Values.ZERO_INT)

  private val numberRunningAvg = new AtomicReference[AvgNumber](AvgNumber())

  override def newUpdater(): Updater = new Upd()
  override def result: AnyValue =
    if (seenDuration) {
      val monthsRunningAvg = durationRunningAvg.get().monthsRunningAvg
      val daysRunningAvg = durationRunningAvg.get().daysRunningAvg
      val secondsRunningAvg = durationRunningAvg.get().secondsRunningAvg
      val nanosRunningAvg = durationRunningAvg.get().nanosRunningAvg
      DurationValue.approximate(monthsRunningAvg, daysRunningAvg, secondsRunningAvg, nanosRunningAvg).normalize()
    } else if (seenNumber) {
      numberRunningAvg.get().avgNumber
    } else {
      Values.NO_VALUE
    }

  private def onNumberUpdate(partAvg: NumberValue, partCount: Long) = {
    seenNumber = true
    if (seenDuration) {
      AvgAggregator.failMix()
    }
    numberRunningAvg.updateAndGet(old => {
      val newCount = old.count + partCount
      val diff = partAvg.minus(old.avgNumber)
      val next = diff.times(partCount.toDouble / newCount)
      AvgNumber(newCount, overflowSafeAdd(old.avgNumber, next))
    })
  }

  private def onDurationUpdate(sumMonths: Long, sumDays: Long, sumSeconds: Long, sumNanos: Long, partCount: Long) = {
    seenDuration = true
    if (seenNumber) {
      AvgAggregator.failMix()
    }
    durationRunningAvg.updateAndGet(old => {
      val newCount = old.count + partCount
      AvgDuration(newCount,
        old.monthsRunningAvg + (sumMonths - old.monthsRunningAvg * partCount) / newCount,
        old.daysRunningAvg + (sumDays - old.daysRunningAvg * partCount) / newCount,
        old.secondsRunningAvg + (sumSeconds - old.secondsRunningAvg * partCount) / newCount,
        old.nanosRunningAvg + (sumNanos - old.nanosRunningAvg * partCount) / newCount)
    })
  }

  class Upd() extends AvgBase with Updater {

    override def add(value: Array[AnyValue]): Unit = update(value(0))
    override def applyUpdates(): Unit = {
      if (seenNumber) {
        onNumberUpdate(avgNumber, partCount)
      } else if (seenDuration) {
        onDurationUpdate(sumMonths, sumDays, sumSeconds, sumNanos, partCount)
      }
      reset()
    }
  }
}
