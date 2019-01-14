/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.vectorized.EmptyQueryState
import org.neo4j.values.storable.Values

import scala.language.postfixOps

class AggregationReducerOperatorNoGroupingTest extends MorselUnitTest {

  test("reduce from single morsel") {
    val aggregation = new AggregationReduceOperatorNoGrouping(workId, Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))

    val input = new Input()
      .addRow(Longs(0), Refs(Values.longArray(Array(2, 4, 42))))

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .addInput(input)
      .withOutput(0 longs, 1 refs, 1 rows)

    given.whenInit().whenOperate
      .shouldReturnRow(Refs(Values.longArray(Array(2, 4, 42))))
      .shouldBeDone()
  }

  test("reduce values from multiple morsels") {
    // Given
    val aggregation = new AggregationReduceOperatorNoGrouping(workId, Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0))))
    val inputs = for (i <- 1 to 10) yield {
      new Input().addRow(Longs(0), Refs(Values.longArray(Array(2 * i))))
    }

    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .withOutput(0 longs, 1 refs, 1 rows)
    inputs.foreach(given.addInput)

    given.whenInit().whenOperate
      .shouldReturnRow(Refs(Values.longArray(Array(2,4,6,8,10,12,14,16,18,20))))
      .shouldBeDone()
  }
}
