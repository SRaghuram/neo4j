/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.RefSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes
import org.neo4j.cypher.internal.runtime.slotted.pipes._
import org.neo4j.cypher.internal.runtime.vectorized.EmptyQueryState
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue

class AggregationMapperOperatorTest extends MorselUnitTest {

  test("single grouping key aggregation") {
    val groupSlot = RefSlot(0, nullable = false, CTAny)
    val aggregation = new AggregationMapperOperator(workId,
                                                    Array(AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression1(groupSlot,
                                                                               new DummyExpression(stringValue("A"), stringValue("B"))))

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(0), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(1), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(2), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(3), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(4), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(5), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(6), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(7), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(8), Refs(Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(9), Refs(Values.NO_VALUE, Values.NO_VALUE))

    given.whenOperate()
      .shouldReturnRow(Longs(0), Refs(stringValue("A"), Values.longArray(Array(0, 2, 4, 6, 8))))
      .shouldReturnRow(Longs(1), Refs(stringValue("B"), Values.EMPTY_LONG_ARRAY))
      .shouldBeDone()
  }

  test("two grouping keys") {
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val aggregation = new AggregationMapperOperator(workId,
                                                    Array(AggregationOffsets(2, 2, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression2(
                                                      groupSlot1, new DummyExpression(stringValue("A"), stringValue("B")),
                                                      groupSlot2, new DummyExpression(stringValue("C"), stringValue("D"),
                                                                          stringValue("E"))))
    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(0), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(1), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(2), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(3), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(4), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(5), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(6), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(7), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(8), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(9), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))

    given.whenOperate()
      .shouldReturnRow(Longs(0), Refs(stringValue("A"), stringValue("C"), Values.longArray(Array(0, 6))))
      .shouldReturnRow(Longs(1), Refs(stringValue("B"), stringValue("D"), Values.EMPTY_LONG_ARRAY))
      .shouldReturnRow(Longs(2), Refs(stringValue("A"), stringValue("E"), Values.longArray(Array(2, 8))))
      .shouldReturnRow(Longs(3), Refs(stringValue("B"), stringValue("C"), Values.EMPTY_LONG_ARRAY))
      .shouldReturnRow(Longs(4), Refs(stringValue("A"), stringValue("D"), Values.longArray(Array(4))))
      .shouldReturnRow(Longs(5), Refs(stringValue("B"), stringValue("E"), Values.EMPTY_LONG_ARRAY))
      .shouldBeDone()

  }

  test("three grouping keys") {
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val aggregation = new AggregationMapperOperator(workId,
                                                    Array(AggregationOffsets(3, 3, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression3(
                                                      groupSlot1, new DummyExpression(stringValue("A"), stringValue("B")),
                                                      groupSlot2, new DummyExpression(stringValue("C"), stringValue("D")),
                                                      groupSlot3, new DummyExpression(stringValue("E"), stringValue("F"))))

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(0), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(1), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(2), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(3), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(4), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(5), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(6), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(7), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(8), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(9), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))

    given.whenOperate()
      .shouldReturnRow(Longs(0), Refs(stringValue("A"), stringValue("C"), stringValue("E"), Values.longArray(Array(0, 2, 4, 6, 8))))
      .shouldReturnRow(Longs(1), Refs(stringValue("B"), stringValue("D"), stringValue("F"), Values.EMPTY_LONG_ARRAY))
      .shouldBeDone()
  }

  test("more than three grouping keys") {
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val groupSlot4 = RefSlot(3, nullable = false, CTAny)
    val groupSlot5 = RefSlot(4, nullable = false, CTAny)
    val aggregation = new AggregationMapperOperator(workId,
                                                    Array(AggregationOffsets(5, 5, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression(Array(
                                                      SlotExpression(groupSlot1, new DummyExpression(stringValue("A"), stringValue("B"))),
                                                      SlotExpression(groupSlot2, new DummyExpression(stringValue("C"), stringValue("D"))),
                                                      SlotExpression(groupSlot3, new DummyExpression(stringValue("E"), stringValue("F"))),
                                                      SlotExpression(groupSlot4, new DummyExpression(stringValue("G"), stringValue("H"))),
                                                      SlotExpression(groupSlot5, new DummyExpression(stringValue("I"), stringValue("J"))))))
    val longs = Array[Long](0,1,2,3,4,5,6,7,8,9)
    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(0), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(1), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(2), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(3), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(4), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(5), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(6), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(7), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(8), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))
      .addInputRow(Longs(9), Refs(Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE))

    given.whenOperate()
      .shouldReturnRow(Longs(0), Refs(stringValue("A"), stringValue("C"), stringValue("E"), stringValue("G"), stringValue("I"), Values.longArray(Array(0, 2, 4, 6, 8))))
      .shouldReturnRow(Longs(1), Refs(stringValue("B"), stringValue("D"), stringValue("F"), stringValue("H"), stringValue("J"), Values.EMPTY_LONG_ARRAY))
      .shouldBeDone()
  }
}
