/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.DistinctFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.SumFunction
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.storable.Values

class StandardSumAggregatorTest extends SumAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = SumAggregator
}

class ConcurrentSumAggregatorTest extends SumAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = SumAggregator
}

class FunctionSumAggregatorTest extends SumAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new SumFunction(e)
}

abstract class SumAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should sum numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.sum))
  }

  test("should sum durations") {
    val result = runSingleAggregation(randomDurationsWithNulls)
    result should be(randomDurations.reduce(_ add _))
  }
}

class StandardSumDistinctAggregatorTest extends SumDistinctAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = SumDistinctAggregator
}

class ConcurrentSumDistinctAggregatorTest extends SumDistinctAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = SumDistinctAggregator
}

class FunctionSumDistinctAggregatorTest extends SumDistinctAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(exp: Expression): AggregationFunction = new DistinctFunction(exp, new SumFunction(exp), EmptyMemoryTracker.INSTANCE)
}

abstract class SumDistinctAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should sum DISTINCT numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.distinct.sum))
  }

  test("should sum DISTINCT durations") {
    val result = runSingleAggregation(randomDurationsWithNulls)
    result should be(randomDurations.distinct.reduce(_ add _))
  }
}
