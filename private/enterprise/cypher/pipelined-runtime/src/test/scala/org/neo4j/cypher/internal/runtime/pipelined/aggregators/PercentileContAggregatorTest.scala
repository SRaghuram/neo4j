/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.PercentileContFunction
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StandardPercentileContAggregatorTest.percentile
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StandardPercentileContAggregatorTest.percentileCont
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

import scala.util.Random

object StandardPercentileContAggregatorTest {
  lazy val percentile: Double = Random.nextDouble()
  def percentileCont(ints: Seq[Int]): Double = {
    val n = ints.length
    val sorted = ints.sorted
    if (percentile == 1.0 || n == 1) {
      sorted.last
    } else  {
      val floatIdx = percentile * (n - 1)
      val floor = floatIdx.toInt
      val ceil = math.ceil(floatIdx).toInt
      if (ceil == floor || floor == n - 1) sorted(floor)
      else sorted(floor) * (ceil - floatIdx) + sorted(ceil) * (floatIdx - floor)
    }
  }
}

class StandardPercentileContAggregatorTest extends PercentileContAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = PercentileContAggregator
}

class ConcurrentPercentileContAggregatorTest extends PercentileContAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = PercentileContAggregator
}

class FunctionPercentileContAggregatorTest extends PercentileContAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Array[Expression]): AggregationFunction =
    new PercentileContFunction(e(0), e(1), EmptyMemoryTracker.INSTANCE)

  override def getAggregationFunction(e: Expression): AggregationFunction = ???
}

abstract class PercentileContAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should compute percentile cont of numbers") {
    val input = randomIntValuesWithNulls.map(i => Array[AnyValue](i, Values.doubleValue(percentile)))
    val result = runAggregation(input).asInstanceOf[NumberValue].doubleValue()
    result should be(percentileCont(randomInts))
  }
}
