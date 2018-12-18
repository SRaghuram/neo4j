/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.RefSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes
import org.neo4j.cypher.internal.runtime.slotted.pipes._
import org.neo4j.cypher.internal.runtime.vectorized.Morsel
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue

import scala.language.postfixOps

class AggregationReducerOperatorTest extends MorselUnitTest {

  test("single grouping key single morsel aggregation") {
    val groupSlot = RefSlot(0, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression1(groupSlot, new DummyExpression()))
    val inputs = for (i <- 1 to 5) yield {
      new Input()
        .addRow(Refs(Values.stringValue("k1"), Values.longArray(Array(2 * i))))
        .addRow(Refs(Values.stringValue("k2"), Values.longArray(Array(20 * i))))
    }

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .withOutput(0 longs, 2 refs, 2 rows)
    inputs.foreach(given.addInput)

    given.whenInit().whenOperate
      .shouldReturnRow(Refs(stringValue("k1"), Values.longArray(Array(2, 4, 6, 8, 10))))
      .shouldReturnRow(Refs(stringValue("k2"), Values.longArray(Array(20, 40, 60, 80, 100))))
      .shouldBeDone()
  }

  test("two grouping keys") {
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(2, 2, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression2(
                                                       groupSlot1, new DummyExpression(),
                                                       groupSlot2, new DummyExpression()))
    val inputs = for (i <- 1 to 5) yield {
      new Input()
        .addRow(Refs(Values.stringValue("k11"), Values.stringValue("k12"), Values.longArray(Array(2 * i))))
        .addRow(Refs(Values.stringValue("k21"), Values.stringValue("k22"), Values.longArray(Array(20 * i))))
    }

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .withOutput(0 longs, 3 refs, 2 rows)
    inputs.foreach(given.addInput)

    given.whenInit().whenOperate
      .shouldReturnRow(Refs(Values.stringValue("k11"), Values.stringValue("k12"), Values.longArray(Array(2, 4, 6, 8, 10))))
      .shouldReturnRow(Refs(Values.stringValue("k21"), Values.stringValue("k22"), Values.longArray(Array(20, 40, 60, 80, 100))))
      .shouldBeDone()
  }

  test("three grouping keys") {
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(3, 3, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression3(
                                                      groupSlot1, new DummyExpression(),
                                                      groupSlot2, new DummyExpression(),
                                                      groupSlot3, new DummyExpression()))
    val inputs = for (i <- 1 to 5) yield {
      new Input()
        .addRow(Refs(Values.stringValue("k11"), Values.stringValue("k12"), Values.stringValue("k13"), Values.longArray(Array(2 * i))))
        .addRow(Refs(Values.stringValue("k21"), Values.stringValue("k22"), Values.stringValue("k23"), Values.longArray(Array(20 * i))))
    }

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .withOutput(0 longs, 4 refs, 2 rows)
    inputs.foreach(given.addInput)

    given.whenInit().whenOperate
      .shouldReturnRow(Refs(Values.stringValue("k11"), Values.stringValue("k12"), Values.stringValue("k13"), Values.longArray(Array(2, 4, 6, 8, 10))))
      .shouldReturnRow(Refs(Values.stringValue("k21"), Values.stringValue("k22"), Values.stringValue("k23"), Values.longArray(Array(20, 40, 60, 80, 100))))
      .shouldBeDone()

  }

  test("more than three grouping keys") {
    val groupSlot1 = RefSlot(0, nullable = false, CTAny)
    val groupSlot2 = RefSlot(1, nullable = false, CTAny)
    val groupSlot3 = RefSlot(2, nullable = false, CTAny)
    val groupSlot4 = RefSlot(3, nullable = false, CTAny)
    val groupSlot5 = RefSlot(4, nullable = false, CTAny)
    val aggregation = new AggregationReduceOperator(workId,
                                                    Array(AggregationOffsets(5, 5, DummyEvenNodeIdAggregation(0))),
                                                    SlottedGroupingExpression(
                                                      Array(SlotExpression(groupSlot1, new DummyExpression()),
                                                            SlotExpression(groupSlot2, new DummyExpression()),
                                                            SlotExpression(groupSlot3, new DummyExpression()),
                                                            SlotExpression(groupSlot4, new DummyExpression()),
                                                            SlotExpression(groupSlot5, new DummyExpression()))))
    val inputs = for (i <- 1 to 5) yield {
      new Input()
        .addRow(Refs(Values.stringValue("k11"), Values.stringValue("k12"), Values.stringValue("k13"), Values.stringValue("k14"), Values.stringValue("k15"), Values.longArray(Array(2 * i))))
        .addRow(Refs(Values.stringValue("k21"), Values.stringValue("k22"), Values.stringValue("k23"), Values.stringValue("k24"), Values.stringValue("k25"), Values.longArray(Array(20 * i))))
    }

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .withOutput(0 longs, 6 refs, 2 rows)
    inputs.foreach(given.addInput)

    given.whenInit().whenOperate
      .shouldReturnRow(Refs(Values.stringValue("k11"), Values.stringValue("k12"), Values.stringValue("k13"), Values.stringValue("k14"), Values.stringValue("k15"), Values.longArray(Array(2, 4, 6, 8, 10))))
      .shouldReturnRow(Refs(Values.stringValue("k21"), Values.stringValue("k22"), Values.stringValue("k23"), Values.stringValue("k24"), Values.stringValue("k25"), Values.longArray(Array(20, 40, 60, 80, 100))))
      .shouldBeDone()
  }
}
