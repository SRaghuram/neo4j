/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.vectorized.EmptyQueryState
import org.neo4j.values.storable.Values

class AggregationMapperOperatorNoGroupingTest extends MorselUnitTest {

  test("single aggregation on a single morsel") {
    val given = new Given()
      .withOperator(new AggregationMapperOperatorNoGrouping(workId, Array(AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0)))))
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(0), Refs(Values.NO_VALUE))
      .addInputRow(Longs(1), Refs(Values.NO_VALUE))
      .addInputRow(Longs(2), Refs(Values.NO_VALUE))
      .addInputRow(Longs(3), Refs(Values.NO_VALUE))
      .addInputRow(Longs(4), Refs(Values.NO_VALUE))
      .addInputRow(Longs(5), Refs(Values.NO_VALUE))
      .addInputRow(Longs(6), Refs(Values.NO_VALUE))
      .addInputRow(Longs(7), Refs(Values.NO_VALUE))
      .addInputRow(Longs(8), Refs(Values.NO_VALUE))
      .addInputRow(Longs(9), Refs(Values.NO_VALUE))

    given.whenOperate()
        .shouldReturnRow(Longs(0), Refs(Values.longArray(Array(0,2,4,6,8))))
        .shouldBeDone()
  }

  test("multiple aggregations on a single morsel") {
    val aggregation = new AggregationMapperOperatorNoGrouping(
      workId,
      Array(
        AggregationOffsets(0, 0, DummyEvenNodeIdAggregation(0)),
        AggregationOffsets(1, 1, DummyEvenNodeIdAggregation(1))
      ))
    val given = new Given()
      .withOperator(aggregation)
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(0, 1), Refs(Values.NO_VALUE))
      .addInputRow(Longs(2, 3), Refs(Values.NO_VALUE))
      .addInputRow(Longs(4, 5), Refs(Values.NO_VALUE))
      .addInputRow(Longs(6, 7), Refs(Values.NO_VALUE))
      .addInputRow(Longs(8, 9), Refs(Values.NO_VALUE))

    given.whenOperate()
      .shouldReturnRow(Longs(0, 1), Refs(Values.longArray(Array(0,2,4,6,8))))
      .shouldBeDone()
  }
}
