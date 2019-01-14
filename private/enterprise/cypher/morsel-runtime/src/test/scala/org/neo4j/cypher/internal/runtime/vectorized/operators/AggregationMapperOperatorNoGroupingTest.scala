/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.vectorized.{Morsel, MorselExecutionContext, QueryState}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class AggregationMapperOperatorNoGroupingTest extends CypherFunSuite {

  test("single aggregation on a single morsel") {
    // Given
    val numberOfLongs = 1
    val numberOfReferences = 1
    val aggregation = new AggregationMapperOperatorNoGrouping(Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val longs = Array[Long](0,1,2,3,4,5,6,7,8,9)
    val refs = new Array[AnyValue](10)
    val data = new Morsel(longs, refs, longs.length)

    // When
    aggregation.operate(MorselExecutionContext(data, numberOfLongs, numberOfReferences), null, QueryState.EMPTY)

    // Then
    data.refs(0) should equal(Values.longArray(Array(0,2,4,6,8)))
  }

  test("multiple aggregations on a single morsel") {
    val numberOfLongs = 2
    val numberOfReferences = 1

    val aggregation = new AggregationMapperOperatorNoGrouping(Array(
      AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0)),
      AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(1))
    ))

    //this is interpreted as n1,n2,n1,n2...
    val longs = Array[Long](0,1,2,3,4,5,6,7,8,9)
    val refs = new Array[AnyValue](5)
    val data = new Morsel(longs, refs, 5)

    aggregation.operate(MorselExecutionContext(data, numberOfLongs, numberOfReferences), null, QueryState.EMPTY)

    data.refs(0) should equal(Values.longArray(Array(0,2,4,6,8)))
    data.refs(1) should equal(Values.longArray(Array.empty))
  }
}
