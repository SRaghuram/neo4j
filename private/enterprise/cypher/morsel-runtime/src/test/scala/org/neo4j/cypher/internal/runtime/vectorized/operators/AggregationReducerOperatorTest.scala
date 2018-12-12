/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.RefSlot
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.parallel.{WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.vectorized.{Morsel, _}
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class AggregationReducerOperatorTest extends CypherFunSuite {

  private val resources = new QueryResources(mock[CursorFactory])

  private val workId: WorkIdentity = WorkIdentityImpl(42, "Work Identity Description")

  test("single grouping key single morsel aggregation") {
    // Given
    val numberOfLongs = 0
    val numberOfReferences = 2
    val groupSlot = RefSlot(0, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot, groupSlot, new DummyExpression())))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](10)
      refs(0) = Values.stringValue("k1")
      refs(1) = Values.longArray(Array(2*i))
      refs(2) = Values.stringValue("k2")
      refs(3) = Values.longArray(Array(20*i))
      val morsel = new Morsel(Array.empty, refs)
      MorselExecutionContext(morsel, numberOfLongs, numberOfReferences, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20))
    // When
    aggregation.init(null, null, in, resources)
          .operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, 2), null, EmptyQueryState(), resources)

    // Then
    out.refs(0) should equal(stringValue("k1"))
    out.refs(1) should equal(Values.longArray(Array(2,4,6,8,10)))
    out.refs(2) should equal(stringValue("k2"))
    out.refs(3) should equal(Values.longArray(Array(20,40,60,80,100)))
  }

  test("two grouping keys") {
    // Given
    val numberOfLongs = 1
    val numberOfReferences = 3
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(2, 2, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot1, groupSlot1,
                                                                          new DummyExpression()),
                                                          GroupingOffsets(groupSlot2, groupSlot2,
                                                                          new DummyExpression())
                                                          ))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](15)
      refs(0) = Values.stringValue("k11")
      refs(1) = Values.stringValue("k12")
      refs(2) = Values.longArray(Array(2*i))
      refs(3) = Values.stringValue("k21")
      refs(4) = Values.stringValue("k22")
      refs(5) = Values.longArray(Array(20*i))
      val morsel = new Morsel(Array.empty, refs)
      MorselExecutionContext(morsel, numberOfLongs, numberOfReferences, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20))
    // When
    aggregation.init(null, null, in, resources)
          .operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, 2), null, EmptyQueryState(), resources)

    // Then
    out.refs(0) should equal(stringValue("k11"))
    out.refs(1) should equal(stringValue("k12"))
    out.refs(2) should equal(Values.longArray(Array(2,4,6,8,10)))
    out.refs(3) should equal(stringValue("k21"))
    out.refs(4) should equal(stringValue("k22"))
    out.refs(5) should equal(Values.longArray(Array(20,40,60,80,100)))
  }

  test("three grouping keys") {
    // Given
    val numberOfLongs = 1
    val numberOfReferences = 4
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(3, 3, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot1, groupSlot1, new DummyExpression()),
                                                          GroupingOffsets(groupSlot2, groupSlot2, new DummyExpression()),
                                                          GroupingOffsets(groupSlot3, groupSlot3, new DummyExpression())
                                                    ))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](20)
      refs(0) = Values.stringValue("k11")
      refs(1) = Values.stringValue("k12")
      refs(2) = Values.stringValue("k13")
      refs(3) = Values.longArray(Array(2*i))
      refs(4) = Values.stringValue("k21")
      refs(5) = Values.stringValue("k22")
      refs(6) = Values.stringValue("k23")
      refs(7) = Values.longArray(Array(20*i))
      val morsel = new Morsel(Array.empty, refs)
      MorselExecutionContext(morsel, numberOfLongs, numberOfReferences, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20))
    // When
    aggregation.init(null, null, in, resources)
          .operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, 2), null, EmptyQueryState(), resources)

    // Then
    out.refs(0) should equal(stringValue("k11"))
    out.refs(1) should equal(stringValue("k12"))
    out.refs(2) should equal(stringValue("k13"))
    out.refs(3) should equal(Values.longArray(Array(2, 4, 6, 8, 10)))
    out.refs(4) should equal(stringValue("k21"))
    out.refs(5) should equal(stringValue("k22"))
    out.refs(6) should equal(stringValue("k23"))
    out.refs(7) should equal(Values.longArray(Array(20, 40, 60, 80, 100)))

  }

  test("more than three grouping keys") {
    // Given
    val numberOfLongs = 1
    val numberOfReferences = 6
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val groupSlot4 = RefSlot(3, nullable = false, CTAny)
    val groupSlot5 = RefSlot(4, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(5, 5, DummyEvenNodeIdAggregation(0))),
                                                    Array(GroupingOffsets(groupSlot1, groupSlot1, new DummyExpression()),
                                                          GroupingOffsets(groupSlot2, groupSlot2, new DummyExpression()),
                                                          GroupingOffsets(groupSlot3, groupSlot3, new DummyExpression()),
                                                          GroupingOffsets(groupSlot4, groupSlot4, new DummyExpression()),
                                                          GroupingOffsets(groupSlot5, groupSlot5, new DummyExpression())
                                                    ))
    val in = 1 to 5 map ( i => {
      val refs = new Array[AnyValue](30)
      refs(0) = Values.stringValue("k11")
      refs(1) = Values.stringValue("k12")
      refs(2) = Values.stringValue("k13")
      refs(3) = Values.stringValue("k14")
      refs(4) = Values.stringValue("k15")
      refs(5) = Values.longArray(Array(2*i))
      refs(6) = Values.stringValue("k21")
      refs(7) = Values.stringValue("k22")
      refs(8) = Values.stringValue("k23")
      refs(9) = Values.stringValue("k24")
      refs(10) = Values.stringValue("k25")
      refs(11) = Values.longArray(Array(20*i))
      val morsel = new Morsel(Array.empty, refs)
      MorselExecutionContext(morsel, numberOfLongs, numberOfReferences, 2)
    })

    val out = new Morsel(Array.empty, new Array[AnyValue](20))
    // When
    aggregation.init(null, null, in, resources)
          .operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, 2), null, EmptyQueryState(), resources)

    // Then
    out.refs(0) should equal(stringValue("k11"))
    out.refs(1) should equal(stringValue("k12"))
    out.refs(2) should equal(stringValue("k13"))
    out.refs(3) should equal(stringValue("k14"))
    out.refs(4) should equal(stringValue("k15"))
    out.refs(5) should equal(Values.longArray(Array(2, 4, 6, 8, 10)))
    out.refs(6) should equal(stringValue("k21"))
    out.refs(7) should equal(stringValue("k22"))
    out.refs(8) should equal(stringValue("k23"))
    out.refs(9) should equal(stringValue("k24"))
    out.refs(10) should equal(stringValue("k25"))
    out.refs(11) should equal(Values.longArray(Array(20, 40, 60, 80, 100)))
  }
}
