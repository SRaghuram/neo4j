/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.LongSlot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.v4_0.util.symbols.CTNode

import scala.language.postfixOps

class MergeSortOperatorTest extends MorselUnitTest {

  test("merge sort a single sorted morsel") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input = new Input()
      .row(Longs(1))
      .row(Longs(2))
      .row(Longs(3))
      .row(Longs(4))
      .row(Longs(5))
      .row(Longs(6))
      .row(Longs(7))
      .row(Longs(8))
      .row(Longs(9))
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input)
      .output(1 longs)
      .output(0 refs)
      .output(9 rows)

    given.whenInit().whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3)).shouldReturnRow(Longs(4)).shouldReturnRow(Longs(5))
      .shouldReturnRow(Longs(6)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(8)).shouldReturnRow(Longs(9))
      .shouldBeDone()
  }

  test("top on a single morsel") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input = new Input()
      .row(Longs(1))
      .row(Longs(2))
      .row(Longs(3))
      .row(Longs(4))
      .row(Longs(5))
      .row(Longs(6))
      .row(Longs(7))
      .row(Longs(8))
      .row(Longs(9))
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering, Some(Literal(3))))
      .addInput(input)
      .output(1 longs)
      .output(0 refs)
      .output(9 rows)
      .state(EmptyQueryState())

    given.whenInit().whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3))
      .shouldBeDone()
  }

  test("merge sort two sorted morsels") {
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val input1 = new Input()
      .row(Longs(1))
      .row(Longs(2))
      .row(Longs(3))
      .row(Longs(4))
      .row(Longs(6))
      .row(Longs(7))
      .row(Longs(8))
      .row(Longs(9))
    val input2 = new Input()
      .row(Longs(5))
      .row(Longs(7))
      .row(Longs(9))
      .row(Longs(14))
      .row(Longs(86))
      .row(Longs(92))
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .output(1 longs)
      .output(0 refs)
      .output(5 rows)

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
      .row(Longs(1, 101))
      .row(Longs(3, 103))
      .row(Longs(5, 105))
      .row(Longs(7, 107))
      .row(Longs(9, 109))
    val input2 = new Input()
      .row(Longs(2, 102))
      .row(Longs(4, 104))
      .row(Longs(6, 106))
      .row(Longs(8, 108))
      .row(Longs(10, 110))
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .output(2 longs)
      .output(0 refs)
      .output(4 rows)

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
      .row(Longs(1, 101))
      .row(Longs(1, 102))
      .row(Longs(2, 202))
      .row(Longs(5, 501))
      .row(Longs(7, 701))
    val input2 = new Input()
      .row(Longs(1, 103))
      .row(Longs(2, 201))
      .row(Longs(3, 301))
      .row(Longs(5, 502))
      .row(Longs(5, 503))
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .output(2 longs, 0 refs, 4 rows)

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
      .row(Longs(1))
      .row(Longs(2))
      .row(Longs(3))
      .row(Longs(4))
      .row(Longs(6))
      .row(Longs(7))
      .row(Longs(8))
      .row(Longs(9))
    val input2 = new Input()
      .row(Longs(5))
      .row(Longs(7))
      .row(Longs(9))
      .row(Longs(14))
      .row(Longs(86))
      .row(Longs(92))
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering, Some(Literal(9))))
      .addInput(input1)
      .addInput(input2)
      .output(1 longs)
      .output(0 refs)
      .output(5 rows)
      .state(EmptyQueryState())

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
      .row(Longs(1))
      .row(Longs(2))
      .row(Longs(3))
      .row(Longs(4))
      .row(Longs(5))
      .row(Longs(6))
      .row(Longs(7))
      .row(Longs(8))
      .row(Longs(9))
    val input2 = new Input()
      .noRows(1,0)
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input1)
      .addInput(input2)
      .output(1 longs)
      .output(0 refs)
      .output(9 rows)

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
      .row(Longs(1))
      .row(Longs(2))
      .row(Longs(3))
      .row(Longs(4))
      .row(Longs(5))
      .row(Longs(6))
      .row(Longs(7))
      .row(Longs(8))
      .row(Longs(9))
    val given = new Given()
      .operator(new MergeSortOperator(workId, columnOrdering))
      .addInput(input)
      .output(1 longs)
      .output(0 refs)
      .output(14 rows)

    val task = given.whenInit()

    task.whenOperate
      .shouldReturnRow(Longs(1)).shouldReturnRow(Longs(2)).shouldReturnRow(Longs(3)).shouldReturnRow(Longs(4)).shouldReturnRow(Longs(5))
      .shouldReturnRow(Longs(6)).shouldReturnRow(Longs(7)).shouldReturnRow(Longs(8)).shouldReturnRow(Longs(9))
      .shouldBeDone()
  }

}
