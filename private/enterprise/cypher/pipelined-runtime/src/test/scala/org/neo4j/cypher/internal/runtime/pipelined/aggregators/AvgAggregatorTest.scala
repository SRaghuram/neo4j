/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.time.temporal.ChronoUnit

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AvgFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.DistinctFunction
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AvgAggregatorTest.avg
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AvgAggregatorTest.cutNanos
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

object AvgAggregatorTest {
  def avg(ints: Seq[Int]): Double = {
    ints.sum.toDouble / ints.length
  }
  def avg(durations: Seq[DurationValue]): DurationValue = {
    durations.reduce(_ add _).div(Values.intValue(durations.length))
  }

  // To avoid failing because of rounding errors
  def cutNanos(duration: DurationValue): DurationValue = {
    val normalized = duration.normalize()
    normalized.sub(DurationValue.duration(0, 0, 0, normalized.get(ChronoUnit.NANOS)))
  }
}

class StandardAvgAggregatorTest extends AvgAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = AvgAggregator
}

class ConcurrentAvgAggregatorTest extends AvgAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = AvgAggregator
}

class FunctionAvgAggregatorTest extends AvgAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new AvgFunction(e)
}

abstract class AvgAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should avg numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(avg(randomInts) +- 0.0001)
  }

  test("should avg durations") {
    val result = runSingleAggregation(randomDurationsWithNulls).asInstanceOf[DurationValue]
    cutNanos(result) should be(cutNanos(avg(randomDurations)))
  }
}

class StandardAvgDistinctAggregatorTest extends AvgDistinctAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = AvgDistinctAggregator
}

class ConcurrentAvgDistinctAggregatorTest extends AvgDistinctAggregatorTest with ConcurrentAggregatorTest {
  override val aggregator: Aggregator = AvgDistinctAggregator
}

class FunctionAvgDistinctAggregatorTest extends AvgDistinctAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(exp: Expression): AggregationFunction = new DistinctFunction(exp, new AvgFunction(exp), EmptyMemoryTracker.INSTANCE)
}

abstract class AvgDistinctAggregatorTest extends CypherFunSuite with AggregatorTest {

  test("should avg DISTINCT numbers") {
    val result = runSingleAggregation(randomIntValuesWithNulls).asInstanceOf[NumberValue].doubleValue()
    result should be(avg(randomInts.distinct) +- 0.0001)
  }

  test("should avg DISTINCT durations") {
    val result = runSingleAggregation(randomDurationsWithNulls).asInstanceOf[DurationValue]
    cutNanos(result) should be(cutNanos(avg(randomDurations.distinct)))
  }
}
