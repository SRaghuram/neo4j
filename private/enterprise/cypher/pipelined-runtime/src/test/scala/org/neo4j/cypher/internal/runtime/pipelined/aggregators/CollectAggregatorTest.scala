/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.CollectFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.DistinctFunction
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

class ConcurrentCollectAggregatorTest extends CypherFunSuite with ConcurrentAggregatorTest {
  override def aggregator: Aggregator = throw new UnsupportedOperationException()

  test("should collect concurrent") {
    val result = runConcurrentAggregator(CollectAggregator, randomIntValuesWithNulls.map(Array[AnyValue](_))).asInstanceOf[ListValue].asArray()
    result should contain theSameElementsAs randomIntValues
  }

  test("should collect all concurrent") {
    val result = runConcurrentAggregator(CollectAllAggregator, randomIntValuesWithNulls.map(Array[AnyValue](_))).asInstanceOf[ListValue].asArray()
    result should contain theSameElementsAs randomIntValuesWithNulls
  }

  test("should collect DISTINCT concurrent") {
    val result = runConcurrentAggregator(CollectDistinctAggregator, randomIntValuesWithNulls.map(Array[AnyValue](_))).asInstanceOf[ListValue].asArray()
    result should contain theSameElementsAs randomIntValues.distinct
  }
}

class StandardCollectAggregatorTest extends CollectAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = CollectAggregator
}

class FunctionCollectAggregatorTest extends CollectAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(e: Expression): AggregationFunction = new CollectFunction(e, EmptyMemoryTracker.INSTANCE)
}

abstract class CollectAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should collect") {
    val result = runAggregation(randomIntValuesWithNulls.map(Array[AnyValue](_)))
    result should be(VirtualValues.list(randomIntValues: _*))
  }
}

class CollectAllAggregatorTest extends CypherFunSuite with AggregatorTest {
  override def runAggregation(values: Seq[Array[AnyValue]]): AnyValue = throw new UnsupportedOperationException()

  test("should collect all standard") {
    val result = runStandardAggregator(CollectAllAggregator, randomIntValuesWithNulls.map(Array[AnyValue](_)))
    result should be(VirtualValues.list(randomIntValuesWithNulls: _*))
  }
}

class StandardCollectDistinctAggregatorTest extends CollectDistinctAggregatorTest with StandardAggregatorTest {
  override val aggregator: Aggregator = CollectDistinctAggregator
}

class FunctionCollectDistinctAggregatorTest extends CollectDistinctAggregatorTest with FunctionAggregatorTest {
  override def getAggregationFunction(exp: Expression): AggregationFunction = new DistinctFunction(exp, new CollectFunction(exp, EmptyMemoryTracker.INSTANCE), EmptyMemoryTracker.INSTANCE)
}

abstract class CollectDistinctAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should collect DISTINCT") {
    val result = runAggregation(randomIntValuesWithNulls.map(Array[AnyValue](_)))
    result should be(VirtualValues.list(randomIntValues.distinct: _*))
  }
}
