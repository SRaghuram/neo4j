/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.slotted.Ascending
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.v4_0.util.symbols.CTNode

import scala.language.postfixOps

class MergeSortOperatorTest extends MorselUnitTest {

  test("merge sort a single sorted morsel") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input = new Input()
      .addRow(Longs(1))
      .addRow(Longs(2))
      .addRow(Longs(3))
      .addRow(Longs(4))
      .addRow(Longs(5))
      .addRow(Longs(6))
      .addRow(Longs(7))
      .addRow(Longs(8))
      .addRow(Longs(9))
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input)
      .withOutput(1 longs)
      .withOutput(0 refs)
      .withOutput(9 rows)

    given.whenInit().whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3)).shouldReturnRow(Longs(4)).shouldReturnRow(Longs(5))
      .shouldReturnRow(Longs(6)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(8)).shouldReturnRow(Longs(9))
      .shouldBeDone()
  }

  test("top on a single morsel") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input = new Input()
      .addRow(Longs(1))
      .addRow(Longs(2))
      .addRow(Longs(3))
      .addRow(Longs(4))
      .addRow(Longs(5))
      .addRow(Longs(6))
      .addRow(Longs(7))
      .addRow(Longs(8))
      .addRow(Longs(9))
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering, Some(Literal(3))))
      .addInput(input)
      .withOutput(1 longs)
      .withOutput(0 refs)
      .withOutput(9 rows)
      .withQueryState(EmptyQueryState())

    given.whenInit().whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3))
      .shouldBeDone()
  }

  test("merge sort two sorted morsels") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input1 = new Input()
      .addRow(Longs(1))
      .addRow(Longs(2))
      .addRow(Longs(3))
      .addRow(Longs(4))
      .addRow(Longs(6))
      .addRow(Longs(7))
      .addRow(Longs(8))
      .addRow(Longs(9))
    val input2 = new Input()
      .addRow(Longs(5))
      .addRow(Longs(7))
      .addRow(Longs(9))
      .addRow(Longs(14))
      .addRow(Longs(86))
      .addRow(Longs(92))
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .withOutput(1 longs)
      .withOutput(0 refs)
      .withOutput(5 rows)

    val task = given.whenInit()

    task.whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3)).shouldReturnRow(Longs(4)).shouldReturnRow(Longs(5))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(6)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(8)).shouldReturnRow(Longs(9))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(9)).shouldReturnRow(Longs(14)).shouldReturnRow(Longs(86)).shouldReturnRow(Longs(92))
      .shouldBeDone()
  }

  test("merge sort two sorted morsels with additional column") {
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1))

    val input1 = new Input()
      .addRow(Longs(1, 101))
      .addRow(Longs(3, 103))
      .addRow(Longs(5, 105))
      .addRow(Longs(7, 107))
      .addRow(Longs(9, 109))
    val input2 = new Input()
      .addRow(Longs(2, 102))
      .addRow(Longs(4, 104))
      .addRow(Longs(6, 106))
      .addRow(Longs(8, 108))
      .addRow(Longs(10, 110))
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .withOutput(2 longs)
      .withOutput(0 refs)
      .withOutput(4 rows)

    val task = given.whenInit()

    task.whenOperate
      .shouldReturnRow(Longs(1, 101))
      .shouldReturnRow(Longs(2, 102))
      .shouldReturnRow(Longs(3, 103))
      .shouldReturnRow(Longs(4, 104))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(5, 105))
      .shouldReturnRow(Longs(6, 106))
      .shouldReturnRow(Longs(7, 107))
      .shouldReturnRow(Longs(8, 108))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(9, 109))
      .shouldReturnRow(Longs(10, 110))
      .shouldBeDone()
  }

  test("merge sort two sorted morsels by two columns") {
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = LongSlot(1, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1), Ascending(slot2))

    val input1 = new Input()
      .addRow(Longs(1, 101))
      .addRow(Longs(1, 102))
      .addRow(Longs(2, 202))
      .addRow(Longs(5, 501))
      .addRow(Longs(7, 701))
    val input2 = new Input()
      .addRow(Longs(1, 103))
      .addRow(Longs(2, 201))
      .addRow(Longs(3, 301))
      .addRow(Longs(5, 502))
      .addRow(Longs(5, 503))
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .withOutput(2 longs, 0 refs, 4 rows)

    val task = given.whenInit()

    task.whenOperate
      .shouldReturnRow(Longs(1, 101))
      .shouldReturnRow(Longs(1, 102))
      .shouldReturnRow(Longs(1, 103))
      .shouldReturnRow(Longs(2, 201))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(2, 202))
      .shouldReturnRow(Longs(3, 301))
      .shouldReturnRow(Longs(5, 501))
      .shouldReturnRow(Longs(5, 502))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(5, 503))
      .shouldReturnRow(Longs(7, 701))
      .shouldBeDone()
  }

  test("top on two morsels") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input1 = new Input()
      .addRow(Longs(1))
      .addRow(Longs(2))
      .addRow(Longs(3))
      .addRow(Longs(4))
      .addRow(Longs(6))
      .addRow(Longs(7))
      .addRow(Longs(8))
      .addRow(Longs(9))
    val input2 = new Input()
      .addRow(Longs(5))
      .addRow(Longs(7))
      .addRow(Longs(9))
      .addRow(Longs(14))
      .addRow(Longs(86))
      .addRow(Longs(92))
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering, Some(Literal(9))))
      .addInput(input1)
      .addInput(input2)
      .withOutput(1 longs)
      .withOutput(0 refs)
      .withOutput(5 rows)
      .withQueryState(EmptyQueryState())

    val task = given.whenInit()

    task.whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3)).shouldReturnRow(Longs(4)).shouldReturnRow(Longs(5))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(6)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(8))
      .shouldBeDone()
  }

  test("sort two morsels with one empty array") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input1 = new Input()
      .addRow(Longs(1))
      .addRow(Longs(2))
      .addRow(Longs(3))
      .addRow(Longs(4))
      .addRow(Longs(5))
      .addRow(Longs(6))
      .addRow(Longs(7))
      .addRow(Longs(8))
      .addRow(Longs(9))
    val input2 = new Input()
      .withNoRows(1,0)
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .withOutput(1 longs)
      .withOutput(0 refs)
      .withOutput(9 rows)

    val task = given.whenInit()

    task.whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3)).shouldReturnRow(Longs(4)).shouldReturnRow(Longs(5))
      .shouldReturnRow(Longs(6)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(8)).shouldReturnRow(Longs(9))
      .shouldBeDone()
  }

  test("sort with too many output slots") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input = new Input()
      .addRow(Longs(1))
      .addRow(Longs(2))
      .addRow(Longs(3))
      .addRow(Longs(4))
      .addRow(Longs(5))
      .addRow(Longs(6))
      .addRow(Longs(7))
      .addRow(Longs(8))
      .addRow(Longs(9))
    val given = new Given()
      .withOperator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input)
      .withOutput(1 longs)
      .withOutput(0 refs)
      .withOutput(14 rows)

    val task = given.whenInit()

    task.whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3)).shouldReturnRow(Longs(4)).shouldReturnRow(Longs(5))
      .shouldReturnRow(Longs(6)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(8)).shouldReturnRow(Longs(9))
      .shouldBeDone()
  }

}
