/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class MinAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should min numbers standard") {
    val result = runStandardAggregator(MinAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.min))
  }

  test("should min numbers concurrent") {
    val result = runConcurrentAggregator(MinAggregator, randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.min))
  }
}
