/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class CountAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should count standard") {
    val result = runStandardAggregator(CountAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.length))
  }

  test("should count concurrent") {
    val result = runConcurrentAggregator(CountAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.length))
  }

  test("should count * standard") {
    val result = runStandardAggregator(CountStarAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomIntValuesWithNulls.length))
  }

  test("should count * concurrent") {
    val result = runConcurrentAggregator(CountStarAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomIntValuesWithNulls.length))
  }

  test("should count DISTINCT standard") {
    val result = runStandardAggregator(CountDistinctAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.distinct.length))
  }

  test("should count DISTINCT concurrent") {
    val result = runConcurrentAggregator(CountDistinctAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.distinct.length))
  }

}
