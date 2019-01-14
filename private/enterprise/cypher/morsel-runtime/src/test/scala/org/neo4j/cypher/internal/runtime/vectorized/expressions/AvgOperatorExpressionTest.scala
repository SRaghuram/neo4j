/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.vectorized.operators.DummyExpression
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.VirtualValues.list
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class AvgOperatorExpressionTest extends CypherFunSuite {

  test("should do average mapping") {
    //given
    val mapper = AvgOperatorExpression(
      new DummyExpression(longValue(1), longValue(2), longValue(3), longValue(4), longValue(5))).createAggregationMapper

    //when
    1 to 5 foreach(_ => mapper.map(mock[MorselExecutionContext], mock[QueryState]))

    //then
    mapper.result should equal(list(longValue(5), longValue(15)))
  }

  test("should handle mapping of no result") {
    //given
    val mapper = AvgOperatorExpression(new DummyExpression()).createAggregationMapper

    //when doing nothing at all

    //then
    mapper.result should equal(list(longValue(0), longValue(0)))
  }

  test("should do average reducing") {
    //given
    val reducer = AvgOperatorExpression(new DummyExpression()).createAggregationReducer

    //when
    reducer.reduce(list(longValue(10), longValue(10)))
    reducer.reduce(list(longValue(5), longValue(10)))

    //then
    reducer.result should equal(Values.doubleValue(4/3.0))
  }

  test("should handle empty average reducing") {
    //given
    val reducer = AvgOperatorExpression(new DummyExpression()).createAggregationReducer

    //when doing absolutely nothing

    //then
    reducer.result should equal(Values.NO_VALUE)
  }
}
