/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.CountFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.CountStarFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.DistinctFunction
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.storable.Values

class StandardCountAggregatorTest extends CountAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = CountAggregator
}

class ConcurrentCountAggregatorTest extends CountAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = CountAggregator
}

class FunctionCountAggregatorTest extends CountAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new CountFunction(e)
}

abstract class CountAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should count") {
    val result = runSingleAggregation(randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.length))
  }
}

class StandardCountStarAggregatorTest extends CountStarAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = CountStarAggregator
}

class ConcurrentCountStarAggregatorTest extends CountStarAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = CountStarAggregator
}

class FunctionCountStarAggregatorTest extends CountStarAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new CountStarFunction()
}

abstract class CountStarAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should count *") {
    val result = runSingleAggregation(randomIntValuesWithNulls)
    result should be(Values.intValue(randomIntValuesWithNulls.length))
  }
}

class StandardCountDistinctAggregatorTest extends CountDistinctAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = CountDistinctAggregator
}

class ConcurrentCountDistinctAggregatorTest extends CountDistinctAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = CountDistinctAggregator
}

class FunctionCountDistinctAggregatorTest extends CountDistinctAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(exp: Expression): AggregationFunction = new DistinctFunction(exp, new CountFunction(exp), EmptyMemoryTracker.INSTANCE)
}

abstract class CountDistinctAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should count DISTINCT") {
    val result = runSingleAggregation(randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.distinct.length))
  }
}
