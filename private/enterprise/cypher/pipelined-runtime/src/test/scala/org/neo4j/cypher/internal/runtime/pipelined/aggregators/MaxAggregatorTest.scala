/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.MaxFunction
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class StandardMaxAggregatorTest extends MaxAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = MaxAggregator
}

class ConcurrentMaxAggregatorTest extends MaxAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = MaxAggregator
}

class FunctionMaxAggregatorTest extends MaxAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new MaxFunction(e)
}

abstract class MaxAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should max numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.max))
  }
}
