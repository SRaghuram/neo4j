/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.parallel.{WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.vectorized.{EmptyQueryState, Morsel, MorselExecutionContext, QueryResources}
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class AggregationReducerOperatorNoGroupingTest extends CypherFunSuite {

  private val resources = new QueryResources(mock[CursorFactory])

  private val workId: WorkIdentity = WorkIdentityImpl(42, "Work Identity Description")

  test("reduce from single morsel") {
    // Given
    val numberOfLongs = 1
    val numberOfReferences = 1
    val aggregation = new AggregationReduceOperatorNoGrouping(workId, Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val refs = new Array[AnyValue](10)
    refs(0) = Values.longArray(Array(2,4,42))
    val in = new Morsel(Array.empty, refs)
    val out = new Morsel(new Array[Long](10), new Array[AnyValue](10))
    // When
    aggregation.init(null, null, Array(MorselExecutionContext(in, numberOfLongs, numberOfReferences, refs.length)), resources)
          .operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, refs.length), null, EmptyQueryState(), resources)

    // Then
    out.refs(0) should equal(Values.longArray(Array(2,4,42)))
  }

  test("reduce values from multiple morsels") {
    // Given
    val numberOfLongs = 1
    val numberOfReferences = 1
    val aggregation = new AggregationReduceOperatorNoGrouping(workId, Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val in = 1 to 10 map ( i => {
      val refs = new Array[AnyValue](10)
      refs(0) = Values.longArray(Array(2*i))
      val morsel = new Morsel(Array.empty, refs)
      MorselExecutionContext(morsel, numberOfLongs, numberOfReferences, refs.length)
    })

    val out = new Morsel(new Array[Long](10), new Array[AnyValue](10))

    // When
    aggregation.init(null, null, in, resources).operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, 10), null, EmptyQueryState(), resources)

    // Then
    out.refs(0) should equal(Values.longArray(Array(2,4,6,8,10,12,14,16,18,20)))
  }
}
