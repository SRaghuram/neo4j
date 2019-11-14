/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselUnitTest
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap

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

      row.setCurrentRow(inputPos)
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

  class SumUntil32() {
    var sum: Long = 0
    def sumAndCheckIfPast32(add: Long): Boolean = {
      sum += add
      sum <= 32
    }
  }
}
