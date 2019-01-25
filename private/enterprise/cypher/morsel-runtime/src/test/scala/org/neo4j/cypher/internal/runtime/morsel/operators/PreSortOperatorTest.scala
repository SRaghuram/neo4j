/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physical_planning.{LongSlot, RefSlot}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.morsel.EmptyQueryState
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.values.storable.Values.intValue

import scala.language.postfixOps

class PreSortOperatorTest extends MorselUnitTest {

  test("sort a morsel with a single long column") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .withOperator(new PreSortOperator(workId, columnOrdering))
      .addInputRow(Longs(9))
      .addInputRow(Longs(8))
      .addInputRow(Longs(7))
      .addInputRow(Longs(6))
      .addInputRow(Longs(5))
      .addInputRow(Longs(4))
      .addInputRow(Longs(3))
      .addInputRow(Longs(2))
      .addInputRow(Longs(1))

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
      .withOperator(new PreSortOperator(workId, columnOrdering))
      .addInputRow(Longs(6), Refs(intValue(6)))
      .addInputRow(Longs(5), Refs(intValue(5)))
      .addInputRow(Longs(4), Refs(intValue(4)))
      .addInputRow(Longs(9), Refs(intValue(9)))
      .addInputRow(Longs(8), Refs(intValue(8)))
      .addInputRow(Longs(7), Refs(intValue(7)))
      .addInputRow(Longs(3), Refs(intValue(3)))
      .addInputRow(Longs(2), Refs(intValue(2)))
      .addInputRow(Longs(1), Refs(intValue(1)))

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
      .withOperator(new PreSortOperator(workId, columnOrdering))
      .addInputRow(Longs(9, 0))
      .addInputRow(Longs(8, 1))
      .addInputRow(Longs(7, 2))
      .addInputRow(Longs(6, 3))
      .addInputRow(Longs(5, 4))
      .addInputRow(Longs(4, 5))
      .addInputRow(Longs(3, 6))
      .addInputRow(Longs(2, 7))
      .addInputRow(Longs(1, 8))

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
      .withOperator(new PreSortOperator(workId, columnOrdering))
      .withNoInputRow(1, 0)

    given.whenOperate().shouldBeDone()
  }

  test("top on a morsel with a single long column") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .withOperator(new PreSortOperator(workId, columnOrdering, Some(Literal(3))))
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(9))
      .addInputRow(Longs(8))
      .addInputRow(Longs(7))
      .addInputRow(Longs(6))
      .addInputRow(Longs(5))
      .addInputRow(Longs(4))
      .addInputRow(Longs(3))
      .addInputRow(Longs(2))
      .addInputRow(Longs(1))

    given.whenOperate()
      .shouldReturnRow(Longs(1))
      .shouldReturnRow(Longs(2))
      .shouldReturnRow(Longs(3))
      .shouldBeDone()
  }

  test("top with n > morselSize on a morsel with a single long column") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .withOperator(new PreSortOperator(workId, columnOrdering, Some(Literal(20))))
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(9))
      .addInputRow(Longs(8))
      .addInputRow(Longs(7))
      .addInputRow(Longs(6))
      .addInputRow(Longs(5))
      .addInputRow(Longs(4))
      .addInputRow(Longs(3))
      .addInputRow(Longs(2))
      .addInputRow(Longs(1))

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
      .withOperator(new PreSortOperator(workId, columnOrdering, Some(Literal(3))))
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(6), Refs(intValue(6)))
      .addInputRow(Longs(5), Refs(intValue(5)))
      .addInputRow(Longs(4), Refs(intValue(4)))
      .addInputRow(Longs(9), Refs(intValue(9)))
      .addInputRow(Longs(8), Refs(intValue(8)))
      .addInputRow(Longs(7), Refs(intValue(7)))
      .addInputRow(Longs(3), Refs(intValue(3)))
      .addInputRow(Longs(2), Refs(intValue(2)))
      .addInputRow(Longs(1), Refs(intValue(1)))

    given.whenOperate()
      .shouldReturnRow(Longs(1), Refs(intValue(1)))
      .shouldReturnRow(Longs(2), Refs(intValue(2)))
      .shouldReturnRow(Longs(3), Refs(intValue(3)))
      .shouldBeDone()
  }

  test("top on a morsel with no valid data") {
    val columnOrdering = Seq(Ascending(LongSlot(0, nullable = false, CTNode)))
    val given = new Given()
      .withOperator(new PreSortOperator(workId, columnOrdering, Some(Literal(3))))
      .withQueryState(EmptyQueryState())
      .withNoInputRow(1, 0)

    given.whenOperate().shouldBeDone()
  }

  test("top with limit 0") {
    val columnOrdering = Seq(Ascending(RefSlot(0, nullable = false, CTNumber)))
    val given = new Given()
      .withOperator(new PreSortOperator(workId, columnOrdering, Some(Literal(0))))
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(6), Refs(intValue(6)))
      .addInputRow(Longs(5), Refs(intValue(5)))

    given.whenOperate()
      .shouldBeDone()
  }

  test("top with limit -1") {
    val columnOrdering = Seq(Ascending(RefSlot(0, nullable = false, CTNumber)))
    val given = new Given()
      .withOperator(new PreSortOperator(workId, columnOrdering, Some(Literal(-1))))
      .withQueryState(EmptyQueryState())
      .addInputRow(Longs(6), Refs(intValue(6)))
      .addInputRow(Longs(5), Refs(intValue(5)))

    given.whenOperate()
      .shouldBeDone()
  }
}
