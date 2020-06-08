/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.time.temporal.ChronoUnit

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

class AvgAggregatorTest extends CypherFunSuite with AggregatorTest {
  def avg(ints: Seq[Int]): Double = {
    ints.sum.toDouble / ints.length
  }
  def avg(durations: Seq[DurationValue]): DurationValue = {
    durations.reduce(_ add _).div(Values.intValue(durations.length))
  }

  // To avoid failing because of rounding errors
  def cutNanos(duration: DurationValue): DurationValue = {
    val normalized = duration.normalize()
    normalized.sub(DurationValue.duration(0, 0, 0, normalized.get(ChronoUnit.NANOS)))
  }

  test("should avg numbers standard") {
    val result = runStandardAggregator(AvgAggregator, randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(avg(randomInts) +- 0.0001)
  }

  test("should avg numbers concurrent") {
    val result = runConcurrentAggregator(AvgAggregator, randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(avg(randomInts) +- 0.0001)
  }

  test("should avg durations standard") {
    val result = runStandardAggregator(AvgAggregator, randomDurationsWithNulls).asInstanceOf[DurationValue]
    cutNanos(result) should be(cutNanos(avg(randomDurations)))
  }

  test("should avg durations concurrent") {
    val result = runConcurrentAggregator(AvgAggregator, randomDurationsWithNulls).asInstanceOf[DurationValue]
    cutNanos(result) should be(cutNanos(avg(randomDurations)))
  }

  test("should avg DISTINCT numbers standard") {
    val result = runStandardAggregator(AvgDistinctAggregator, randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(avg(randomInts.distinct) +- 0.0001)
  }

  test("should avg DISTINCT numbers concurrent") {
    val result = runConcurrentAggregator(AvgDistinctAggregator, randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(avg(randomInts.distinct) +- 0.0001)
  }

  test("should avg DISTINCT durations standard") {
    val result = runStandardAggregator(AvgDistinctAggregator, randomDurationsWithNulls).asInstanceOf[DurationValue]
    cutNanos(result) should be(cutNanos(avg(randomDurations.distinct)))
  }

  test("should avg DISTINCT durations concurrent") {
    val result = runConcurrentAggregator(AvgDistinctAggregator, randomDurationsWithNulls).asInstanceOf[DurationValue]
    cutNanos(result) should be(cutNanos(avg(randomDurations.distinct)))
  }
}
