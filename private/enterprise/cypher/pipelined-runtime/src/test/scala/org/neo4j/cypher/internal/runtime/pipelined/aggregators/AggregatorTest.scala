/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ThreadLocalRandom

import org.neo4j.cypher.internal.runtime.QueryMemoryTracker.NO_MEMORY_TRACKER
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

trait AggregatorTest {

  private val random: ThreadLocalRandom = ThreadLocalRandom.current()
  private val randomValues: RandomValues = RandomValues.create(random)

  private def runAggregator(aggregator: Aggregator, reducer: Reducer, values: Seq[AnyValue]): AnyValue = {
    val upperBound = Math.max(2, values.size / 10)
    val groups = values.grouped(random.nextInt(1, upperBound))
    val updaters = groups.map { group =>
      val updater = aggregator.newUpdater
      group.foreach(updater.update)
      updater
    }
    updaters.foreach(reducer.update)
    reducer.result
  }

  def runStandardAggregator(aggregator: Aggregator, values: Seq[AnyValue]): AnyValue =
    runAggregator(aggregator, aggregator.newStandardReducer(NO_MEMORY_TRACKER, Id.INVALID_ID), values)

  def runConcurrentAggregator(aggregator: Aggregator, values: Seq[AnyValue]): AnyValue =
    runAggregator(aggregator, aggregator.newConcurrentReducer, values)

  protected val randomInts: Seq[Int] = random.ints(50, 0, 15).toArray
  protected val randomIntValues: Seq[Value] = randomInts.map(Values.intValue)
  protected val randomIntValuesWithNulls: Seq[Value] = randomIntValues.take(25) ++ Seq.fill(7)(Values.NO_VALUE) ++ randomIntValues.drop(25)

  protected val randomDurations: Seq[DurationValue] = {
    val a = Seq.fill(30)(randomValues.nextDuration())
    val b = Seq.fill(20)(a(random.nextInt(a.size)))
    a ++ b
  }
  protected val randomDurationsWithNulls: Seq[Value] = randomDurations.take(25) ++ Seq.fill(7)(Values.NO_VALUE) ++ randomDurations.drop(25)
}
