/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.DistinctFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.StdevFunction
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StdevAggregatorTest.stdev
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.storable.NumberValue

object StdevAggregatorTest {
  // using BigDecimal, because this naive implementation would otherwise
  // accumulate large rounding errors.
  def stdev(ints: Seq[Int]): Double = {
    val n = ints.length
    val sum = BigDecimal(ints.sum)
    val sumSq = BigDecimal(ints.map(x => x * x).sum)
    val variance = (sumSq - (sum * sum) / n) / (n - 1)
    variance.bigDecimal.sqrt(variance.mc).doubleValue()
  }
}

class StandardStdevAggregatorTest extends StdevAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = StdevAggregator
}

class ConcurrentStdevAggregatorTest extends StdevAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = StdevAggregator
}

class FunctionStdevAggregatorTest extends StdevAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new StdevFunction(e, false)
}

abstract class StdevAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should stdev numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(stdev(randomInts) +- 0.0001)
  }
}

class StandardStdevDistinctAggregatorTest extends StdevDistinctAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = StdevDistinctAggregator
}

class ConcurrentStdevDistinctAggregatorTest extends StdevDistinctAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = StdevDistinctAggregator
}

class FunctionStdevDistinctAggregatorTest extends StdevDistinctAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(exp: Expression): AggregationFunction = new DistinctFunction(exp, new StdevFunction(exp, false), EmptyMemoryTracker.INSTANCE)
}

abstract class StdevDistinctAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should stdev DISTINCT numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(stdev(randomInts.distinct) +- 0.0001)
  }
}
