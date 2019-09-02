/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.aggregators

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference

import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{DurationValue, NumberValue, Values}
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
  override def newStandardReducer(memoryTracker: QueryMemoryTracker): Reducer = new AvgStandardReducer
  override def newConcurrentReducer: Reducer = new AvgConcurrentReducer

  class AvgUpdater() extends AvgStandardBase with Updater {

    private[AvgAggregator] var seenNumber = false
    private[AvgAggregator] var seenDuration = false

    private[AvgAggregator] var count: Long = 0L

    private[AvgAggregator] var avgNumber: NumberValue = Values.ZERO_INT

    private[AvgAggregator] var sumMonths = 0L
    private[AvgAggregator] var sumDays = 0L
    private[AvgAggregator] var sumSeconds = 0L
    private[AvgAggregator] var sumNanos = 0L

    override def update(value: AnyValue): Unit = {
      if (value eq Values.NO_VALUE)
        return

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

  class AvgStandardReducer() extends AvgStandardBase with Reducer {

    private[AvgAggregator] var seenNumber = false
    private[AvgAggregator] var seenDuration = false

    private[AvgAggregator] var count: Long = 0L

    private[AvgAggregator] var avgNumber: NumberValue = Values.ZERO_INT

    private[AvgAggregator] var monthsRunningAvg = 0d
    private[AvgAggregator] var daysRunningAvg = 0d
    private[AvgAggregator] var secondsRunningAvg = 0d
    private[AvgAggregator] var nanosRunningAvg = 0d

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
            monthsRunningAvg  += (u.sumMonths -   monthsRunningAvg * u.count) / newCount
            daysRunningAvg    += (u.sumDays -       daysRunningAvg * u.count) / newCount
            secondsRunningAvg += (u.sumSeconds - secondsRunningAvg * u.count) / newCount
            nanosRunningAvg   += (u.sumNanos -     nanosRunningAvg * u.count) / newCount
          }
          count = newCount
      }

    override def result: AnyValue =
      if (seenDuration)
        DurationValue.approximate(monthsRunningAvg, daysRunningAvg, secondsRunningAvg, nanosRunningAvg).normalize()
      else if (seenNumber)
        avgNumber
      else Values.NO_VALUE
  }

  abstract class AvgStandardBase {

    protected def failMix() =
      throw new CypherTypeException("avg() cannot mix number and duration")

    protected def failType(value: AnyValue) =
      throw new CypherTypeException(s"avg() can only handle numerical values, duration, and null. Got $value")
  }

  class AvgConcurrentReducer() extends AvgStandardBase with Reducer {

    // Note: these volatile flags are set by multiple threads, but only to true, so racing is benign.
    @volatile private var seenNumber = false
    @volatile private var seenDuration = false

    case class AvgNumber(count: Long = 0L,
                         avgNumber: NumberValue = Values.ZERO_INT)
    private val number = new AtomicReference[AvgNumber](AvgNumber())

    case class AvgDuration(count: Long = 0L,
                           monthsRunningAvg: Double = 0d,
                           daysRunningAvg: Double = 0d,
                           secondsRunningAvg: Double = 0d,
                           nanosRunningAvg: Double = 0d)
    private val duration = new AtomicReference[AvgDuration](AvgDuration())

    override def update(updater: Updater): Unit =
      updater match {
        case u: AvgUpdater =>
          if (u.seenNumber) {
            seenNumber = true
            if (seenDuration) {
              failMix()
            }
            number.updateAndGet(old => {
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
            duration.updateAndGet(old => {
              val newCount = old.count + u.count
              AvgDuration(newCount,
                          old.monthsRunningAvg  + (u.sumMonths -   old.monthsRunningAvg * u.count) / newCount,
                          old.daysRunningAvg    + (u.sumDays -       old.daysRunningAvg * u.count) / newCount,
                          old.secondsRunningAvg + (u.sumSeconds - old.secondsRunningAvg * u.count) / newCount,
                          old.nanosRunningAvg   + (u.sumNanos -     old.nanosRunningAvg * u.count) / newCount)
            })
          }
      }

    override def result: AnyValue =
      if (seenDuration) {
        val x = duration.get()
        DurationValue.approximate(x.monthsRunningAvg,
                                  x.daysRunningAvg,
                                  x.secondsRunningAvg,
                                  x.nanosRunningAvg).normalize()

      } else if (seenNumber) {
        val x = number.get()
        x.avgNumber

      } else {
        Values.NO_VALUE
      }
  }
}

