/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.PercentileDiscFunction
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StandardPercentileDiscAggregatorTest.percentile
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StandardPercentileDiscAggregatorTest.percentileDisc
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

import scala.util.Random

object StandardPercentileDiscAggregatorTest {
  lazy val percentile: Double = Random.nextDouble()
  def percentileDisc(ints: Seq[Int]): Double = {
    val n = ints.length
    val sorted = ints.sorted
    if (percentile == 1.0 || n == 1) {
      sorted.last
    } else  {
      val floatIdx = percentile * n
      var idx = floatIdx.toInt
      idx = if (floatIdx != idx || idx == 0) idx
      else idx - 1
      sorted(idx)
    }
  }
}

class StandardPercentileDiscAggregatorTest extends PercentileDiscAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = PercentileDiscAggregator
}

class ConcurrentPercentileDiscAggregatorTest extends PercentileDiscAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = PercentileDiscAggregator
}

class FunctionPercentileDiscAggregatorTest extends PercentileDiscAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Array[Expression]): AggregationFunction =
    new PercentileDiscFunction(e(0), e(1), EmptyMemoryTracker.INSTANCE)

  override def getAggregationFunction(e: Expression): AggregationFunction = ???
}

abstract class PercentileDiscAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should compute percentile disc of numbers") {
    val input = randomIntValuesWithNulls.map(i => Array[AnyValue](i, Values.doubleValue(percentile)))
    val result = runAggregation(input).asInstanceOf[NumberValue].doubleValue()
    result should be(percentileDisc(randomInts))
  }
}
