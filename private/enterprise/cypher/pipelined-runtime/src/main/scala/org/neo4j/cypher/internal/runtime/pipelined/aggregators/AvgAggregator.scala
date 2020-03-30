/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference

import org.neo4j.exceptions.CypherTypeException
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
  override def newUpdater: Updater = new AvgUpdater
  override def newStandardReducer(memoryTracker: MemoryTracker): Reducer = new AvgStandardReducer
  override def newConcurrentReducer: Reducer = new AvgConcurrentReducer
}

case object AvgDistinctAggregator extends Aggregator {
  override def newUpdater: Updater = new DistinctUpdater
  override def newStandardReducer(memoryTracker: QueryMemoryTracker, operatorId: Id): Reducer = new DistinctStandardReducer(new AvgDistinctStandardReducer)
  override def newConcurrentReducer: Reducer = new DistinctConcurrentReducer(new AvgDistinctConcurrentReducer)
}

abstract class AvgStandardBase {
  protected def failMix() =
    throw new CypherTypeException("avg() cannot mix number and duration")

  protected def failType(value: AnyValue) =
    throw new CypherTypeException(s"avg() can only handle numerical values, duration, and null. Got $value")
}

class AvgUpdater() extends AvgStandardBase with Updater {

  private[aggregators] var seenNumber = false
  private[aggregators] var seenDuration = false

  private[aggregators] var count: Long = 0L

  private[aggregators] var avgNumber: NumberValue = Values.ZERO_INT

  private[aggregators] var sumMonths = 0L
  private[aggregators] var sumDays = 0L
  private[aggregators] var sumSeconds = 0L
  private[aggregators] var sumNanos = 0L

  override def update(value: AnyValue): Unit = {
    if (value eq Values.NO_VALUE) {
      return
    }
    count += 1

    value match {
      case number: NumberValue =>
        seenNumber = true
        if (!seenDuration) {
          val diff = number.minus(avgNumber)
          val next = diff.dividedBy(count.toDouble)
          avgNumber = overflowSafeAdd(avgNumber, next)
        } else {
          failMix()
        }

      case dur: DurationValue =>
        seenDuration = true
        if (!seenNumber) {
          sumMonths += dur.get(ChronoUnit.MONTHS)
          sumDays += dur.get(ChronoUnit.DAYS)
          sumSeconds += dur.get(ChronoUnit.SECONDS)
          sumNanos += dur.get(ChronoUnit.NANOS)
        } else {
          failMix()
        }

      case _ => failType(value)
    }
  }
}

trait AvgResultProvider {
  private[aggregators] def seenNumber: Boolean
  private[aggregators] def seenDuration: Boolean
  private[aggregators] def monthsRunningAvg: Double
  private[aggregators] def daysRunningAvg: Double
  private[aggregators] def secondsRunningAvg: Double
  private[aggregators] def nanosRunningAvg: Double
  private[aggregators] def avgNumber: NumberValue

  def result: AnyValue =
    if (seenDuration) {
      DurationValue.approximate(monthsRunningAvg, daysRunningAvg, secondsRunningAvg, nanosRunningAvg).normalize()
    } else if (seenNumber) {
      avgNumber
    } else {
      Values.NO_VALUE
    }
}

trait AvgStandardReducerBase extends AvgResultProvider {
  private[aggregators] var seenNumber = false
  private[aggregators] var seenDuration = false

  private[aggregators] var count: Long = 0L

  private[aggregators] var avgNumber: NumberValue = Values.ZERO_INT

  private[aggregators] var monthsRunningAvg = 0d
  private[aggregators] var daysRunningAvg = 0d
  private[aggregators] var secondsRunningAvg = 0d
  private[aggregators] var nanosRunningAvg = 0d
}

class AvgStandardReducer() extends AvgStandardBase with AvgStandardReducerBase with Reducer {
  override def update(updater: Updater): Unit =
    updater match {
      case u: AvgUpdater =>
        val newCount = count + u.count
        if (u.seenNumber) {
          seenNumber = true
          if (seenDuration) {
            failMix()
          }
          val diff = u.avgNumber.minus(avgNumber)
          val next = diff.times(u.count.toDouble / newCount)
          avgNumber = overflowSafeAdd(avgNumber, next)

        } else if (u.seenDuration) {
          seenDuration = true
          if (seenNumber) {
            failMix()
          }
          monthsRunningAvg += (u.sumMonths - monthsRunningAvg * u.count) / newCount
          daysRunningAvg += (u.sumDays - daysRunningAvg * u.count) / newCount
          secondsRunningAvg += (u.sumSeconds - secondsRunningAvg * u.count) / newCount
          nanosRunningAvg += (u.sumNanos - nanosRunningAvg * u.count) / newCount
        }
        count = newCount
    }
}

trait AvgConcurrentReducerBase extends AvgResultProvider {
  // Note: these volatile flags are set by multiple threads, but only to true, so racing is benign.
  @volatile private[aggregators] var seenNumber = false
  @volatile private[aggregators] var seenDuration = false


  case class AvgDuration(count: Long = 0L,
                         monthsRunningAvg: Double = 0d,
                         daysRunningAvg: Double = 0d,
                         secondsRunningAvg: Double = 0d,
                         nanosRunningAvg: Double = 0d)

  private[aggregators] val durationRunningAvg = new AtomicReference[AvgDuration](AvgDuration())

  override private[aggregators] def monthsRunningAvg = durationRunningAvg.get().monthsRunningAvg
  override private[aggregators] def daysRunningAvg = durationRunningAvg.get().daysRunningAvg
  override private[aggregators] def secondsRunningAvg = durationRunningAvg.get().secondsRunningAvg
  override private[aggregators] def nanosRunningAvg = durationRunningAvg.get().nanosRunningAvg

  case class AvgNumber(count: Long = 0L,
                       avgNumber: NumberValue = Values.ZERO_INT)

  private[aggregators] val numberRunningAvg = new AtomicReference[AvgNumber](AvgNumber())

  override private[aggregators] def avgNumber = numberRunningAvg.get().avgNumber
}

class AvgConcurrentReducer() extends AvgStandardBase with AvgConcurrentReducerBase with Reducer {
  override def update(updater: Updater): Unit =
    updater match {
      case u: AvgUpdater =>
        if (u.seenNumber) {
          seenNumber = true
          if (seenDuration) {
            failMix()
          }
          numberRunningAvg.updateAndGet(old => {
            val newCount = old.count + u.count
            val diff = u.avgNumber.minus(old.avgNumber)
            val next = diff.times(u.count.toDouble / newCount)
            AvgNumber(newCount, overflowSafeAdd(old.avgNumber, next))
          })

        } else if (u.seenDuration) {
          seenDuration = true
          if (seenNumber) {
            failMix()
          }
          durationRunningAvg.updateAndGet(old => {
            val newCount = old.count + u.count
            AvgDuration(newCount,
              old.monthsRunningAvg + (u.sumMonths - old.monthsRunningAvg * u.count) / newCount,
              old.daysRunningAvg + (u.sumDays - old.daysRunningAvg * u.count) / newCount,
              old.secondsRunningAvg + (u.sumSeconds - old.secondsRunningAvg * u.count) / newCount,
              old.nanosRunningAvg + (u.sumNanos - old.nanosRunningAvg * u.count) / newCount)
          })
        }
    }
}

class AvgDistinctStandardReducer() extends AvgStandardBase with DistinctInnerReducer with AvgStandardReducerBase {

  override def update(value: AnyValue): Unit = {
    count += 1
    value match {
      case number: NumberValue =>
        seenNumber = true
        if (!seenDuration) {
          val diff = number.minus(avgNumber)
          val next = diff.dividedBy(count.toDouble)
          avgNumber = overflowSafeAdd(avgNumber, next)
        } else {
          failMix()
        }

      case duration: DurationValue =>
        seenDuration = true
        if (!seenNumber) {
          monthsRunningAvg += (duration.get(ChronoUnit.MONTHS).asInstanceOf[Double] - monthsRunningAvg) / count
          daysRunningAvg += (duration.get(ChronoUnit.DAYS).asInstanceOf[Double] - daysRunningAvg) / count
          secondsRunningAvg += (duration.get(ChronoUnit.SECONDS).asInstanceOf[Double] - secondsRunningAvg) / count
          nanosRunningAvg += (duration.get(ChronoUnit.NANOS).asInstanceOf[Double] - nanosRunningAvg) / count
        } else {
          failMix()
        }

      case _ => failType(value)
    }
  }
}

class AvgDistinctConcurrentReducer() extends AvgStandardBase with AvgConcurrentReducerBase with DistinctInnerReducer {
  override def update(value: AnyValue): Unit = {
    value match {
      case number: NumberValue =>
        seenNumber = true
        if (!seenDuration) {
          numberRunningAvg.updateAndGet(old => {
            val newCount = old.count + 1
            val diff = number.minus(old.avgNumber)
            val next = diff.dividedBy(newCount.toDouble)
            AvgNumber(newCount, overflowSafeAdd(old.avgNumber, next))
          })
        } else {
          failMix()
        }

      case duration: DurationValue =>
        seenDuration = true
        if (!seenNumber) {
          durationRunningAvg.updateAndGet(old => {
            val newCount = old.count + 1
            AvgDuration(newCount,
              old.monthsRunningAvg + (duration.get(ChronoUnit.MONTHS).asInstanceOf[Double] - old.monthsRunningAvg) / newCount,
              old.daysRunningAvg + (duration.get(ChronoUnit.DAYS).asInstanceOf[Double] - old.daysRunningAvg) / newCount,
              old.secondsRunningAvg + (duration.get(ChronoUnit.SECONDS).asInstanceOf[Double] - old.secondsRunningAvg) / newCount,
              old.nanosRunningAvg + (duration.get(ChronoUnit.NANOS).asInstanceOf[Double] - old.nanosRunningAvg) / newCount)
          })
        } else {
          failMix()
        }

      case _ => failType(value)
    }
  }
}
