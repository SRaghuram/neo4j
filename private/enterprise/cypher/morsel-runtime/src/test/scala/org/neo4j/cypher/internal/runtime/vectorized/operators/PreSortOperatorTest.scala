/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.LongSlot
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.RefSlot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized.EmptyQueryState
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.values.storable.Values.intValue

import scala.language.postfixOps

class PreSortOperatorTest extends MorselUnitTest {

  test("sort a morsel with a single long column") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering))
      .inputRow(Longs(9))
      .inputRow(Longs(8))
      .inputRow(Longs(7))
      .inputRow(Longs(6))
      .inputRow(Longs(5))
      .inputRow(Longs(4))
      .inputRow(Longs(3))
      .inputRow(Longs(2))
      .inputRow(Longs(1))

    given.whenOperate()
        .shouldReturnRow(Longs(1))
        .shouldReturnRow(Longs(2))
        .shouldReturnRow(Longs(3))
        .shouldReturnRow(Longs(4))
        .shouldReturnRow(Longs(5))
        .shouldReturnRow(Longs(6))
        .shouldReturnRow(Longs(7))
        .shouldReturnRow(Longs(8))
        .shouldReturnRow(Longs(9))
        .shouldBeDone()
  }

  test("sort a morsel with a one long slot and one ref slot, order by ref") {
    val columnOrdering = Seq(Ascending(RefSlot(0, nullable = false, CTNumber)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering))
      .inputRow(Longs(6), Refs(intValue(6)))
      .inputRow(Longs(5), Refs(intValue(5)))
      .inputRow(Longs(4), Refs(intValue(4)))
      .inputRow(Longs(9), Refs(intValue(9)))
      .inputRow(Longs(8), Refs(intValue(8)))
      .inputRow(Longs(7), Refs(intValue(7)))
      .inputRow(Longs(3), Refs(intValue(3)))
      .inputRow(Longs(2), Refs(intValue(2)))
      .inputRow(Longs(1), Refs(intValue(1)))

    given.whenOperate()
      .shouldReturnRow(Longs(1), Refs(intValue(1)))
      .shouldReturnRow(Longs(2), Refs(intValue(2)))
      .shouldReturnRow(Longs(3), Refs(intValue(3)))
      .shouldReturnRow(Longs(4), Refs(intValue(4)))
      .shouldReturnRow(Longs(5), Refs(intValue(5)))
      .shouldReturnRow(Longs(6), Refs(intValue(6)))
      .shouldReturnRow(Longs(7), Refs(intValue(7)))
      .shouldReturnRow(Longs(8), Refs(intValue(8)))
      .shouldReturnRow(Longs(9), Refs(intValue(9)))
      .shouldBeDone()
  }

  test("sort a morsel with a two long columns by one") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering))
      .inputRow(Longs(9, 0))
      .inputRow(Longs(8, 1))
      .inputRow(Longs(7, 2))
      .inputRow(Longs(6, 3))
      .inputRow(Longs(5, 4))
      .inputRow(Longs(4, 5))
      .inputRow(Longs(3, 6))
      .inputRow(Longs(2, 7))
      .inputRow(Longs(1, 8))

    given.whenOperate()
      .shouldReturnRow(Longs(1, 8))
      .shouldReturnRow(Longs(2, 7))
      .shouldReturnRow(Longs(3, 6))
      .shouldReturnRow(Longs(4, 5))
      .shouldReturnRow(Longs(5, 4))
      .shouldReturnRow(Longs(6, 3))
      .shouldReturnRow(Longs(7, 2))
      .shouldReturnRow(Longs(8, 1))
      .shouldReturnRow(Longs(9, 0))
  }

  test("sort a morsel with no valid data") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering))
      .noInputRow(1, 0)

    given.whenOperate().shouldBeDone()
  }

  test("top on a morsel with a single long column") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering, Some(Literal(3))))
      .state(EmptyQueryState())
      .inputRow(Longs(9))
      .inputRow(Longs(8))
      .inputRow(Longs(7))
      .inputRow(Longs(6))
      .inputRow(Longs(5))
      .inputRow(Longs(4))
      .inputRow(Longs(3))
      .inputRow(Longs(2))
      .inputRow(Longs(1))

    given.whenOperate()
      .shouldReturnRow(Longs(1))
      .shouldReturnRow(Longs(2))
      .shouldReturnRow(Longs(3))
      .shouldBeDone()
  }

  test("top with n > morselSize on a morsel with a single long column") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering, Some(Literal(20))))
      .state(EmptyQueryState())
      .inputRow(Longs(9))
      .inputRow(Longs(8))
      .inputRow(Longs(7))
      .inputRow(Longs(6))
      .inputRow(Longs(5))
      .inputRow(Longs(4))
      .inputRow(Longs(3))
      .inputRow(Longs(2))
      .inputRow(Longs(1))

    given.whenOperate()
      .shouldReturnRow(Longs(1))
      .shouldReturnRow(Longs(2))
      .shouldReturnRow(Longs(3))
      .shouldReturnRow(Longs(4))
      .shouldReturnRow(Longs(5))
      .shouldReturnRow(Longs(6))
      .shouldReturnRow(Longs(7))
      .shouldReturnRow(Longs(8))
      .shouldReturnRow(Longs(9))
      .shouldBeDone()
  }

  test("top on a morsel with a one long slot and one ref slot, order by ref") {
    val columnOrdering = Seq(Ascending(RefSlot(0, nullable = false, CTNumber)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering, Some(Literal(3))))
      .state(EmptyQueryState())
      .inputRow(Longs(6), Refs(intValue(6)))
      .inputRow(Longs(5), Refs(intValue(5)))
      .inputRow(Longs(4), Refs(intValue(4)))
      .inputRow(Longs(9), Refs(intValue(9)))
      .inputRow(Longs(8), Refs(intValue(8)))
      .inputRow(Longs(7), Refs(intValue(7)))
      .inputRow(Longs(3), Refs(intValue(3)))
      .inputRow(Longs(2), Refs(intValue(2)))
      .inputRow(Longs(1), Refs(intValue(1)))

    given.whenOperate()
      .shouldReturnRow(Longs(1), Refs(intValue(1)))
      .shouldReturnRow(Longs(2), Refs(intValue(2)))
      .shouldReturnRow(Longs(3), Refs(intValue(3)))
      .shouldBeDone()
  }

  test("top on a morsel with no valid data") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .operator(new PreSortOperator(workId, columnOrdering, Some(Literal(3))))
      .state(EmptyQueryState())
      .noInputRow(1, 0)

    given.whenOperate().shouldBeDone()
  }
}
