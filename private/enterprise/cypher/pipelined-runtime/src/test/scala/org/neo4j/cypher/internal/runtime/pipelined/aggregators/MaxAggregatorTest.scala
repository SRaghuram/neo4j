/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class MaxAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should max numbers standard") {
    val result = runStandardAggregator(MaxAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.max))
  }

  test("should max numbers concurrent") {
    val result = runConcurrentAggregator(MaxAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.max))
  }
}
