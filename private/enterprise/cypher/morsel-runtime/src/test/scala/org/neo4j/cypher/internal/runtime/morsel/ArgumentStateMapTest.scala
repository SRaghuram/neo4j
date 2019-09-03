/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.runtime.morsel.execution.{FilteringMorselExecutionContext, MorselExecutionContext}
import org.neo4j.cypher.internal.runtime.morsel.operators.MorselUnitTest
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap
import org.neo4j.values.storable.Values

class ArgumentStateMapTest extends MorselUnitTest {
  Seq(0, 1, 2, 3, 5, 8, 13, 21).foreach { numberOfRows => {
    Seq((alwaysTruePredicate, "always true"),
      (alwaysFalsePredicate, "always false"),
      (moduloPredicate(2), "modulo 2"),
      (moduloPredicate(3), "modulo 3"),
      (moduloPredicate(4), "modulo 4"),
      (ltPredicate(numberOfRows / 2), "first half"),
      (gtePredicate(numberOfRows / 2), "second half"),
      (eqPredicate(0), "first row"),
      (eqPredicate(numberOfRows - 1), "last row")).foreach {
      case (predicate, name) =>
        test(s"filter with predicate - $name - $numberOfRows rows") {
          // Given
          val row = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter(row, morselFilterPredicate(predicate))
          // Then
          validateRows(row, numberOfRows, predicate)
        }

        test(s"filter with argument state - $name - $numberOfRows rows") {
          // Given
          val row = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter[FILTER_STATE](0, row, onArgumentFilterPredicate(predicate), onRowFilterPredicate)
          // Then
          validateRows(row, numberOfRows, predicate)
        }

        test(s"filterCancelledArguments - $name - $numberOfRows rows") {
          // Given
          val row = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filterCancelledArguments(0, row, filterCancelledArgumentsCheck(predicate))
          // Then
          validateRows(row, numberOfRows, predicate)
        }
    }
  }
  }

  // Chain two filters. The second filter will be applied to an already filtered morsel.
  Seq(0, 1, 2, 3, 5, 8, 13, 21).foreach { numberOfRows =>
    Seq((ltPredicate(numberOfRows / 2), moduloPredicate(2), "first half -> modulo 2"),
      (ltPredicate(numberOfRows / 2), moduloPredicate(3), "first half -> modulo 3"),
      (ltPredicate(numberOfRows / 2), ltPredicate(numberOfRows / 4), "first half -> first half"),
      (gtePredicate(numberOfRows / 2), moduloPredicate(2), "second half -> modulo 2"),
      (gtePredicate(numberOfRows / 2), moduloPredicate(2), "second half -> modulo 3"),
      (gtePredicate(numberOfRows / 2), gtePredicate(numberOfRows - (numberOfRows / 4)), "second half -> second half"),
      (alwaysFalsePredicate, alwaysFalsePredicate, "always false -> always false"),
      (alwaysFalsePredicate, alwaysTruePredicate, "always false -> always true"),
      (alwaysTruePredicate, alwaysTruePredicate, "always true -> always true"),
      (alwaysTruePredicate, alwaysFalsePredicate, "always true -> always false")).foreach {
      case (p1, p2, name) =>
        test(s"filter with predicate - $name - $numberOfRows rows") {
          // Given
          val row = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter(row, morselFilterPredicate(p1))
          ArgumentStateMap.filter(row, morselFilterPredicate(p2))
          // Then
          validateRows(row, numberOfRows, x => p1(x) && p2(x))
        }

        test(s"filter with argument state - $name - $numberOfRows rows") {
          // Given
          val row = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter(row, morselFilterPredicate(p1))
          ArgumentStateMap.filter(row, morselFilterPredicate(p2))
          // Then
          validateRows(row, numberOfRows, x => p1(x) && p2(x))
        }

        test(s"filterCancelledArguments - $name - $numberOfRows rows") {
          // Given
          val row = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filterCancelledArguments(0, row, filterCancelledArgumentsCheck(p1))
          ArgumentStateMap.filterCancelledArguments(0, row, filterCancelledArgumentsCheck(p2))
          // Then
          validateRows(row, numberOfRows, x => p1(x) && p2(x))
        }
    }
  }

  for (inputPos <- Seq(0, 2, 4, 5, 6, 7)) {

    test(s"filter should work with current row $inputPos") {
      val row = new FilteringInput()
        .addRow(Longs(1, 10))
        .addRow(Longs(1, 11))
        .addRow(Longs(1, 12))
        .addRow(Longs(2, 20))
        .addRow(Longs(3, 30))
        .addRow(Longs(5, 50))
        .addRow(Longs(5, 51))
        .build()

      row.moveToRow(inputPos)
      ArgumentStateMap.filter[SumUntil32](0, row,
        (_, _) => new SumUntil32(),
        (state, currentRow) => state.sumAndCheckIfPast32(currentRow.getLongAt(1)))

      row.getCurrentRow should be(inputPos)
      new ThenOutput(row, 2, 0)
        .shouldReturnRow(Longs(1, 10))
        .shouldReturnRow(Longs(1, 11))
        .shouldReturnRow(Longs(2, 20))
        .shouldReturnRow(Longs(3, 30))
        .shouldBeDone()
    }
  }

  for (inputPos <- Seq(0, 3, 4, 5, 6, 7)) {

    test(s"filterCancelledArguments should work with current row $inputPos") {
      val row = new FilteringInput()
        .addRow(Longs(1, 10))
        .addRow(Longs(1, 11))
        .addRow(Longs(1, 12))
        .addRow(Longs(2, 20))
        .addRow(Longs(3, 30))
        .addRow(Longs(6, 50))
        .addRow(Longs(6, 51))
        .build()

      row.moveToRow(inputPos)
      ArgumentStateMap.filterCancelledArguments(0,
        row,
        argumentRowId => argumentRowId % 2 == 0)

      row.getCurrentRow should be(inputPos)
      new ThenOutput(row, 2, 0)
        .shouldReturnRow(Longs(1, 10))
        .shouldReturnRow(Longs(1, 11))
        .shouldReturnRow(Longs(1, 12))
        .shouldReturnRow(Longs(3, 30))
        .shouldBeDone()
    }
  }

  def alwaysTruePredicate: Long => Boolean = _ => true
  def alwaysFalsePredicate: Long => Boolean = _ => false
  def moduloPredicate(n: Long): Long => Boolean = _ % n == 0
  def ltPredicate(n: Long): Long => Boolean = _ < n
  def gtePredicate(n: Long): Long => Boolean = _ >= n
  def eqPredicate(n: Long): Long => Boolean = _ == n

  //-------------------
  // Creates a predicate for ArgumentStateMaps.filter(morsel: MorselExecutionContext, predicate: MorselExecutionContext => Boolean)
  def morselFilterPredicate(predicate: Long => Boolean): MorselExecutionContext => Boolean = {
    m => predicate(m.getLongAt(0))
  }

  //-------------------
  // Creates predicates for ArgumentStateMaps.filter[FILTER_STATE](argumentSlotOffset: Int,
  //                                                               morsel: MorselExecutionContext,
  //                                                               onArgument: (Long, Long) => FILTER_STATE,
  //                                                               onRow: (FILTER_STATE, MorselExecutionContext) => Boolean)
  type FILTER_STATE = Long

  def onArgumentFilterPredicate(predicate: Long => Boolean): (Long, Long) => FILTER_STATE =
    (argumentRowId, nRows) =>
      if (predicate(argumentRowId))
        argumentRowId
      else
        -1L

  val onRowFilterPredicate: (FILTER_STATE, MorselExecutionContext) => Boolean =
    (state, row) =>
      row.getLongAt(0) == state

  //-------------------
  // Creates an isCancelledCheck for ArgumentStateMaps.filterCancelledArguments(argumentSlotOffset: Int, morsel: MorselExecutionContext, isCancelledCheck: Long => Boolean)
  def filterCancelledArgumentsCheck(predicate: Long => Boolean): Long => Boolean = !predicate(_)

  def buildSequentialInput(numberOfRows: Int): FilteringMorselExecutionContext = {
    var rb = new FilteringInput()
    (0 until numberOfRows).foreach { i =>
      rb = rb.addRow(Longs(i, i*2), Refs(Values.stringValue(i.toString), Values.stringValue((i*2).toString)))
    }
    rb.build()
  }

  def validateRowDataContent(row: MorselExecutionContext, i: Int): Unit = {
    row.getLongAt(0) shouldEqual i
    row.getLongAt(1) shouldEqual i*2
    row.getRefAt(0) shouldEqual Values.stringValue(i.toString)
    row.getRefAt(1) shouldEqual Values.stringValue((i*2).toString)
  }

  def validateRows(row: FilteringMorselExecutionContext, numberOfRows: Int, predicate: Long => Boolean): Unit = {
    val rawRow = row.shallowCopy()

    val expectedValidRows = (0 until numberOfRows).foldLeft(0)((count, i) => if (predicate(i)) count + 1 else count)

    row.getCurrentRow shouldEqual 0
    row.numberOfRows shouldEqual numberOfRows
    rawRow.getCurrentRow shouldEqual 0
    rawRow.numberOfRows shouldEqual numberOfRows

    row.getValidRows shouldEqual expectedValidRows
    rawRow.getValidRows shouldEqual expectedValidRows

    row.resetToFirstRow()
    (0 until numberOfRows).foreach { i =>
      rawRow.isValidRawRow shouldBe true
      if (predicate(i)) {
        rawRow.isCancelled(i) shouldBe false
        row.isValidRow shouldBe true
        validateRowDataContent(row, i)
        validateRowDataContent(rawRow, i)

        val hasNextRow = row.hasNextRow
        row.moveToNextRow()
        row.isValidRow shouldEqual hasNextRow
      } else {
        rawRow.isCancelled(i) shouldBe true
        rawRow.isValidRow shouldBe false
      }
      rawRow.moveToNextRawRow()
    }
    row.isValidRow shouldEqual false
    rawRow.isValidRow shouldEqual false
    rawRow.isValidRawRow shouldEqual false
  }

  class SumUntil32() {
    var sum: Long = 0
    def sumAndCheckIfPast32(add: Long): Boolean = {
      sum += add
      sum <= 32
    }
  }
}
