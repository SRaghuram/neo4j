/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

class CollectAggregatorTest extends CypherFunSuite with AggregatorTest {
  test("should collect standard") {
    val result = runStandardAggregator(CollectAggregator, randomIntValuesWithNulls)
    result should be(VirtualValues.list(randomIntValues: _*))
  }

  test("should collect concurrent") {
    val result = runConcurrentAggregator(CollectAggregator, randomIntValuesWithNulls).asInstanceOf[ListValue].asArray()
    result should contain theSameElementsAs randomIntValues
  }
  test("should collect all standard") {
    val result = runStandardAggregator(CollectAllAggregator, randomIntValuesWithNulls)
    result should be(VirtualValues.list(randomIntValuesWithNulls: _*))
  }

  test("should collect all concurrent") {
    val result = runConcurrentAggregator(CollectAllAggregator, randomIntValuesWithNulls).asInstanceOf[ListValue].asArray()
    result should contain theSameElementsAs randomIntValuesWithNulls
  }

  test("should collect DISTINCT standard") {
    val result = runStandardAggregator(CollectDistinctAggregator, randomIntValuesWithNulls)
    result should be(VirtualValues.list(randomIntValues.distinct: _*))
  }

  test("should collect DISTINCT concurrent") {
    val result = runConcurrentAggregator(CollectDistinctAggregator, randomIntValuesWithNulls).asInstanceOf[ListValue].asArray()
    result should contain theSameElementsAs randomIntValues.distinct
  }
}
