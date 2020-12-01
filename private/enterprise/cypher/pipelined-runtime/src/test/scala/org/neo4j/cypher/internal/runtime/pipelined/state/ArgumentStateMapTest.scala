/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselUnitTest

import scala.collection.mutable.ArrayBuffer

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
          val morsel = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter(morsel, morselFilterPredicate(predicate))
          // Then
          validateRows(morsel, numberOfRows, predicate)
        }

        test(s"filter with argument state - $name - $numberOfRows rows") {
          // Given
          val morsel = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter[FILTER_STATE](0, morsel, onArgumentFilterPredicate(predicate), onRowFilterPredicate)
          // Then
          validateRows(morsel, numberOfRows, predicate)
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
          val morsel = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter(morsel, morselFilterPredicate(p1))
          ArgumentStateMap.filter(morsel, morselFilterPredicate(p2))
          // Then
          validateRows(morsel, numberOfRows, x => p1(x) && p2(x))
        }

        test(s"filter with argument state - $name - $numberOfRows rows") {
          // Given
          val morsel = buildSequentialInput(numberOfRows)
          // When
          ArgumentStateMap.filter(morsel, morselFilterPredicate(p1))
          ArgumentStateMap.filter(morsel, morselFilterPredicate(p2))
          // Then
          validateRows(morsel, numberOfRows, x => p1(x) && p2(x))
        }
    }
  }

  Seq(0, 1, 2, 3, 5, 8, 13, 21).foreach { numberOfRows => {
    test(s"skip once - $numberOfRows rows") {
      // Given
      val morsel = buildSequentialInput(numberOfRows)
      // When
      ArgumentStateMap.skip(morsel, 2)
      // Then
      asList(morsel) should equal((0 until numberOfRows).drop(2))
    }

    test(s"skip twice - $numberOfRows rows") {
      // Given
      val morsel = buildSequentialInput(numberOfRows)
      // When
      ArgumentStateMap.skip(morsel, 2)
      ArgumentStateMap.skip(morsel, 3)
      // Then
      asList(morsel) should equal((0 until numberOfRows).drop(5))
    }

    test(s"filter + skip - $numberOfRows rows") {
      // Given
      val morsel = buildSequentialInput(numberOfRows)
      // When
      ArgumentStateMap.filter(morsel, morselFilterPredicate(moduloPredicate(2)))
      ArgumentStateMap.skip(morsel, 1)
      // Then
      asList(morsel) should equal((0 until numberOfRows).filter(_ % 2 == 0).drop(1))
    }

    test(s"skip + filter - $numberOfRows rows") {
      // Given
      val morsel = buildSequentialInput(numberOfRows)
      // When
      ArgumentStateMap.skip(morsel, 1)
      ArgumentStateMap.filter(morsel, morselFilterPredicate(moduloPredicate(2)))
      // Then
      asList(morsel) should equal((0 until numberOfRows).drop(1).filter(_ % 2 == 0))
    }
  }}

  private def asList(morsel: Morsel): Seq[Long] = {
    val cursor: MorselReadCursor = morsel.readCursor()
    val list = ArrayBuffer.empty[Long]
    while (cursor.next()) list += cursor.getLongAt(0)
    list
  }

  test(s"filter should work with current row") {
    val row = new FilteringInput()
      .addRow(Longs(1, 10))
      .addRow(Longs(1, 11))
      .addRow(Longs(1, 12))
      .addRow(Longs(2, 20))
      .addRow(Longs(3, 30))
      .addRow(Longs(5, 50))
      .addRow(Longs(5, 51))
      .build()

    ArgumentStateMap.filter[SumUntil32](0, row,
                                        (_, _) => new SumUntil32(),
                                        (state, currentRow) => state.sumAndCheckIfPast32(currentRow.getLongAt(1)))

    new ThenOutput(row, 2, 0)
      .shouldReturnRow(Longs(1, 10))
      .shouldReturnRow(Longs(1, 11))
      .shouldReturnRow(Longs(2, 20))
      .shouldReturnRow(Longs(3, 30))
      .shouldBeDone()
  }

  test("skip should work with current row") {
    val row = new FilteringInput()
      .addRow(Longs(1, 10))
      .addRow(Longs(1, 11))
      .addRow(Longs(1, 12))
      .addRow(Longs(2, 20))
      .addRow(Longs(3, 30))
      .addRow(Longs(5, 50))
      .addRow(Longs(5, 51))
      .build()

    //give back one less than asked for
    ArgumentStateMap.skip(0, row, (_, askingFor) => askingFor - 1)

    new ThenOutput(row, 2, 0)
      .shouldReturnRow(Longs(1, 12))
      .shouldReturnRow(Longs(2, 20))
      .shouldReturnRow(Longs(3, 30))
      .shouldReturnRow(Longs(5, 51))
      .shouldBeDone()
  }

  test("filter out even with current row") {
    val row = new FilteringInput()
      .addRow(Longs(1, 1))
      .addRow(Longs(1, 2))
      .addRow(Longs(1, 3))
      .addRow(Longs(1, 4))
      .addRow(Longs(1, 5))
      .addRow(Longs(1, 6))
      .addRow(Longs(1, 7))
      .addRow(Longs(1, 8))
      .addRow(Longs(2, 2))
      .addRow(Longs(2, 4))
      .addRow(Longs(2, 6))
      .addRow(Longs(2, 8))
      .addRow(Longs(2, 10))
      .addRow(Longs(3, 1))
      .addRow(Longs(3, 3))
      .addRow(Longs(3, 5))
      .addRow(Longs(3, 7))
      .addRow(Longs(5, 1))
      .addRow(Longs(5, 2))
      .addRow(Longs(5, 3))
      .build()

    ArgumentStateMap.filter[FilterEven](0, row,
                                        (_, _) => new FilterEven(),
                                        (state, currentRow) => state.isEven(currentRow.getLongAt(1)))

    new ThenOutput(row, 2, 0)
      .shouldReturnRow(Longs(1, 2))
      .shouldReturnRow(Longs(1, 4))
      .shouldReturnRow(Longs(1, 6))
      .shouldReturnRow(Longs(1, 8))
      .shouldReturnRow(Longs(2, 2))
      .shouldReturnRow(Longs(2, 4))
      .shouldReturnRow(Longs(2, 6))
      .shouldReturnRow(Longs(2, 8))
      .shouldReturnRow(Longs(2, 10))
      .shouldReturnRow(Longs(5, 2))
      .shouldBeDone()
  }

  test("filter out even twice with current row") {
    val row = new FilteringInput()
      .addRow(Longs(1, 1))
      .addRow(Longs(1, 2))
      .addRow(Longs(1, 3))
      .addRow(Longs(1, 4))
      .addRow(Longs(1, 5))
      .addRow(Longs(1, 6))
      .addRow(Longs(1, 7))
      .addRow(Longs(1, 8))
      .addRow(Longs(2, 2))
      .addRow(Longs(2, 4))
      .addRow(Longs(2, 6))
      .addRow(Longs(2, 8))
      .addRow(Longs(2, 10))
      .addRow(Longs(3, 1))
      .addRow(Longs(3, 3))
      .addRow(Longs(3, 5))
      .addRow(Longs(3, 7))
      .addRow(Longs(5, 1))
      .addRow(Longs(5, 2))
      .addRow(Longs(5, 3))
      .build()

    ArgumentStateMap.filter[FilterEven](0, row,
                                        (_, _) => new FilterEven(),
                                        (state, currentRow) => state.isEven(currentRow.getLongAt(1)))
    ArgumentStateMap.filter[FilterEven](0, row,
                                        (_, _) => new FilterEven(),
                                        (state, currentRow) => state.isEven(currentRow.getLongAt(1)))

    new ThenOutput(row, 2, 0)
      .shouldReturnRow(Longs(1, 2))
      .shouldReturnRow(Longs(1, 4))
      .shouldReturnRow(Longs(1, 6))
      .shouldReturnRow(Longs(1, 8))
      .shouldReturnRow(Longs(2, 2))
      .shouldReturnRow(Longs(2, 4))
      .shouldReturnRow(Longs(2, 6))
      .shouldReturnRow(Longs(2, 8))
      .shouldReturnRow(Longs(2, 10))
      .shouldReturnRow(Longs(5, 2))
      .shouldBeDone()
  }

  test("filter out even then odd with current row") {
    val row = new FilteringInput()
      .addRow(Longs(1, 1))
      .addRow(Longs(1, 2))
      .addRow(Longs(1, 3))
      .addRow(Longs(1, 4))
      .addRow(Longs(1, 5))
      .addRow(Longs(1, 6))
      .addRow(Longs(1, 7))
      .addRow(Longs(1, 8))
      .addRow(Longs(2, 2))
      .addRow(Longs(2, 4))
      .addRow(Longs(2, 6))
      .addRow(Longs(2, 8))
      .addRow(Longs(2, 10))
      .addRow(Longs(3, 1))
      .addRow(Longs(3, 3))
      .addRow(Longs(3, 5))
      .addRow(Longs(3, 7))
      .addRow(Longs(5, 1))
      .addRow(Longs(5, 2))
      .addRow(Longs(5, 3))
      .build()

    ArgumentStateMap.filter[FilterEven](0, row,
                                        (_, _) => new FilterEven(),
                                        (state, currentRow) => state.isEven(currentRow.getLongAt(1)))
    ArgumentStateMap.filter[FilterEven](0, row,
                                        (_, _) => new FilterEven(),
                                        (state, currentRow) => !state.isEven(currentRow.getLongAt(1)))

    new ThenOutput(row, 2, 0)
      .shouldBeDone()
  }

  test("skip should work after filtering") {
    val row = new FilteringInput()
      .addRow(Longs(1, 1)) //0
      .addRow(Longs(1, 2)) //1
      .addRow(Longs(1, 3)) //2
      .addRow(Longs(1, 4)) //3
      .addRow(Longs(1, 5)) //4
      .addRow(Longs(1, 6)) //5
      .addRow(Longs(1, 7)) //6
      .addRow(Longs(1, 8)) //7
      .addRow(Longs(2, 2)) //8
      .addRow(Longs(2, 4)) //9
      .addRow(Longs(2, 6)) //10
      .addRow(Longs(2, 8)) //11
      .addRow(Longs(2, 10))//12
      .addRow(Longs(3, 1)) //13
      .addRow(Longs(3, 3)) //14
      .addRow(Longs(3, 5)) //15
      .addRow(Longs(3, 7)) //16
      .addRow(Longs(5, 1)) //17
      .addRow(Longs(5, 2)) //18
      .addRow(Longs(5, 3)) //19
      .build()

    ArgumentStateMap.filter[FilterEven](0, row,
                                        (_, _) => new FilterEven(),
                                        (state, currentRow) => state.isEven(currentRow.getLongAt(1)))
    ArgumentStateMap.skip(0, row, (_, askingFor) => math.min(askingFor, 2))

    new ThenOutput(row, 2, 0)
      .shouldReturnRow(Longs(1, 6))
      .shouldReturnRow(Longs(1, 8))
      .shouldReturnRow(Longs(2, 6))
      .shouldReturnRow(Longs(2, 8))
      .shouldReturnRow(Longs(2, 10))
      .shouldBeDone()
  }

  test("skip twice should work with current row") {
    val row = new FilteringInput()
      .addRow(Longs(1, 10))
      .addRow(Longs(1, 11))
      .addRow(Longs(1, 12))
      .addRow(Longs(2, 20))
      .addRow(Longs(3, 30))
      .addRow(Longs(5, 50))
      .addRow(Longs(5, 51))
      .build()

    //give back one less than asked for
    ArgumentStateMap.skip(0, row, (_, askingFor) => askingFor - 1)
    //give back all that was asked for
    ArgumentStateMap.skip(0, row, (_, askingFor) => askingFor)

    new ThenOutput(row, 2, 0)
      .shouldBeDone()
  }

  //-------------------
  // Creates a predicate for ArgumentStateMaps.filter(morsel: MorselExecutionContext, predicate: MorselExecutionContext => Boolean)
  def morselFilterPredicate(predicate: Long => Boolean): ReadWriteRow => Boolean = {
    m => predicate(m.getLongAt(0))
  }

  //-------------------
  // Creates predicates for ArgumentStateMaps.filter[FILTER_STATE](argumentSlotOffset: Int,
  //                                                               morsel: MorselExecutionContext,
  //                                                               onArgument: (Long, Long) => FILTER_STATE,
  //                                                               onRow: (FILTER_STATE, MorselExecutionContext) => Boolean)
  type FILTER_STATE = Long

  def onArgumentFilterPredicate(predicate: Long => Boolean): (Long, Int) => FILTER_STATE =
    (argumentRowId, nRows) =>
      if (predicate(argumentRowId))
        argumentRowId
      else
        -1L

  val onRowFilterPredicate: (FILTER_STATE, ReadWriteRow) => Boolean =
    (state, row) =>
      row.getLongAt(0) == state

  //-------------------
  // Creates an isCancelledCheck for ArgumentStateMaps.filterCancelledArguments(argumentSlotOffset: Int, morsel: MorselExecutionContext, isCancelledCheck: Long => Boolean)
  def filterCancelledArgumentsCheck(predicate: Long => Boolean): Long => Boolean = !predicate(_)

  class SumUntil32() {
    var sum: Long = 0
    def sumAndCheckIfPast32(add: Long): Boolean = {
      sum += add
      sum <= 32
    }
  }

  class FilterEven() {
    def isEven(n: Long): Boolean = n % 2 == 0
  }
}
