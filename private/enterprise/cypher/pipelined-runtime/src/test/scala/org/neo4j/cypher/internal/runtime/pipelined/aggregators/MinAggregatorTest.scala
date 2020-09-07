/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.MinFunction
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class StandardMinAggregatorTest extends MinAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = MinAggregator
}

class ConcurrentMinAggregatorTest extends MinAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = MinAggregator
}

class FunctionMinAggregatorTest extends MinAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new MinFunction(e)
}

abstract class MinAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should min numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls)
    result should be(Values.intValue(randomInts.min))
  }
}
