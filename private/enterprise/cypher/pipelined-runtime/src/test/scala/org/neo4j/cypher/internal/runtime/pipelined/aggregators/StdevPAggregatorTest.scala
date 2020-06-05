/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.DistinctFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.StdevFunction
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StdevPAggregatorTest.stdevP
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.storable.NumberValue

object StdevPAggregatorTest {
  // using BigDecimal, because this naive implementation would otherwise
  // accumulate large rounding errors.
  def stdevP(ints: Seq[Int]): Double = {
    val n = ints.length
    val sum = BigDecimal(ints.sum)
    val sumSq = BigDecimal(ints.map(x => x * x).sum)
    val variance = (sumSq - (sum * sum) / n) / n
    variance.bigDecimal.sqrt(variance.mc).doubleValue()
  }
}

class StandardStdevPAggregatorTest extends StdevPAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = StdevPAggregator
}

class ConcurrentStdevPAggregatorTest extends StdevPAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = StdevPAggregator
}

class FunctionStdevPAggregatorTest extends StdevPAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new StdevFunction(e, true)
}

abstract class StdevPAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should stdevP numbers") {
    val result = runAggregation(randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(stdevP(randomInts) +- 0.0001)
  }
}

class StandardStdevPDistinctAggregatorTest extends StdevPDistinctAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = StdevPDistinctAggregator
}

class ConcurrentStdevPDistinctAggregatorTest extends StdevPDistinctAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = StdevPDistinctAggregator
}

class FunctionStdevPDistinctAggregatorTest extends StdevPDistinctAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(exp: Expression): AggregationFunction = new DistinctFunction(exp, new StdevFunction(exp, true), EmptyMemoryTracker.INSTANCE)
}

abstract class StdevPDistinctAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should stdevP DISTINCT numbers") {
    val result = runAggregation(randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(stdevP(randomInts.distinct) +- 0.0001)
  }
}
