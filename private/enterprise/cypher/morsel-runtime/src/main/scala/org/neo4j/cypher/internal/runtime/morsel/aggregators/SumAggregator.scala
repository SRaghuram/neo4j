/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.aggregators

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{DurationValue, NumberValue, Values}
import org.neo4j.values.utils.ValueMath.overflowSafeAdd


/**
  * Aggregator for sum(...).
  */
case object SumAggregator extends Aggregator {

  override def newUpdater: Updater = new SumUpdater
  override def newStandardReducer(memoryTracker: QueryMemoryTracker): Reducer = new SumStandardReducer
  override def newConcurrentReducer: Reducer = new SumConcurrentReducer

  class SumUpdater() extends SumStandardBase with Updater {

    override def update(value: AnyValue): Unit = {
      if (value eq Values.NO_VALUE)
        return

      value match {
        case number: NumberValue => onNumber(number)
        case dur: DurationValue => onDuration(dur)
        case _ => failType(value)
      }
    }
  }

  class SumStandardReducer() extends SumStandardBase with Reducer {

    override def update(updater: Updater): Unit =
      updater match {
        case u: SumUpdater =>
          if (u.seenNumber) onNumber(u.sumNumber)
          else if (u.seenDuration) onDuration(u.sumDuration)
      }

    override def result: AnyValue =
      if (seenDuration) sumDuration
      else sumNumber
  }

  abstract class SumStandardBase {

    private[SumAggregator] var seenNumber = false
    private[SumAggregator] var seenDuration = false
    private[SumAggregator] var sumNumber: NumberValue = Values.ZERO_INT
    private[SumAggregator] var sumDuration: DurationValue = DurationValue.ZERO

    protected def onNumber(number: NumberValue): Unit = {
      seenNumber = true
      if (!seenDuration)
        sumNumber = overflowSafeAdd(sumNumber, number)
      else
        failMix()
    }

    protected def onDuration(dur: DurationValue): Unit = {
      seenDuration = true
      if (!seenNumber)
        sumDuration = sumDuration.add(dur)
      else
        failMix()
    }

    protected def failMix() =
      throw new CypherTypeException("sum() cannot mix number and duration")

    protected def failType(value: AnyValue) =
      throw new CypherTypeException(s"sum() can only handle numerical values, duration, and null. Got $value")
  }

  class SumConcurrentReducer() extends Reducer {

    // Note: these volatile flags are set by multiple threads, but only to true, so racing is benign.
    @volatile private var seenNumber = false
    @volatile private var seenDuration = false

    private val sumNumber = new AtomicReference[NumberValue](Values.ZERO_INT)
    private val sumDuration = new AtomicReference[DurationValue](DurationValue.ZERO)

    override def update(updater: Updater): Unit =
      updater match {
        case u: SumUpdater =>
          if (u.seenNumber) {
            sumNumber.updateAndGet(num => overflowSafeAdd(num, u.sumNumber))
            seenNumber = true
          } else if (u.seenDuration) {
            sumDuration.updateAndGet(dur => dur.add(u.sumDuration))
            seenDuration = true
          }
      }

    override def result: AnyValue =
      if (seenNumber && seenDuration)
        throw new CypherTypeException("sum() cannot mix number and duration")
      else if (seenDuration)
        sumDuration.get()
      else
        sumNumber.get()
  }
}

