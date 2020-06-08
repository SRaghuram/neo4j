/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class SumAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should sum numbers standard") {
    val result = runStandardAggregator(SumAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.sum))
  }

  test("should sum numbers concurrent") {
    val result = runConcurrentAggregator(SumAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.sum))
  }

  test("should sum durations standard") {
    val result = runStandardAggregator(SumAggregator, randomDurationsWithNulls)
    result should be(randomDurations.reduce(_ add _))
  }

  test("should sum durations concurrent") {
    val result = runConcurrentAggregator(SumAggregator, randomDurationsWithNulls)
    result should be(randomDurations.reduce(_ add _))
  }

  test("should sum DISTINCT numbers standard") {
    val result = runStandardAggregator(SumDistinctAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.distinct.sum))
  }

  test("should sum DISTINCT numbers concurrent") {
    val result = runConcurrentAggregator(SumDistinctAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.distinct.sum))
  }

  test("should sum DISTINCT durations standard") {
    val result = runStandardAggregator(SumDistinctAggregator, randomDurationsWithNulls)
    result should be(randomDurations.distinct.reduce(_ add _))
  }

  test("should sum DISTINCT durations concurrent") {
    val result = runConcurrentAggregator(SumDistinctAggregator, randomDurationsWithNulls)
    result should be(randomDurations.distinct.reduce(_ add _))
  }
}
