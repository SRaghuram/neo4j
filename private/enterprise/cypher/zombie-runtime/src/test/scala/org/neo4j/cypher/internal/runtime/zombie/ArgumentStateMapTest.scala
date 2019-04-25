/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.morsel.operators.MorselUnitTest
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap

class ArgumentStateMapTest extends MorselUnitTest {

  for ((inputPos, outputPos) <- Seq(0 -> 0,
                                    2 -> 2,
                                    4 -> 3,
                                    5 -> 4,
                                    6 -> 4)) {

    test(s"filter should work, while changing current row from $inputPos to $outputPos") {
      val (morsel, row) = new Input()
        .addRow(Longs(1, 10))
        .addRow(Longs(1, 11))
        .addRow(Longs(1, 12))
        .addRow(Longs(2, 20))
        .addRow(Longs(3, 30))
        .addRow(Longs(5, 50))
        .addRow(Longs(5, 51))
        .build

      row.moveToRow(inputPos)
      ArgumentStateMap.filter[SumState](0, row,
                                        (_, _) => new SumState(),
                                        (state, currentRow) => state.keepGoing(currentRow.getLongAt(1)))

      row.getCurrentRow should be(outputPos)
      new ThenOutput(morsel, row, 2, 0)
        .shouldReturnRow(Longs(1, 10))
        .shouldReturnRow(Longs(1, 11))
        .shouldReturnRow(Longs(2, 20))
        .shouldReturnRow(Longs(3, 30))
        .shouldBeDone()
    }
  }

  for ((inputPos, outputPos) <- Seq(0 -> 0,
                                    3 -> 3,
                                    4 -> 3,
                                    5 -> 4,
                                    6 -> 4)) {

    test(s"filterCancelledArguments should work, while changing current row from $inputPos to $outputPos") {
      val (morsel, row) = new Input()
        .addRow(Longs(1, 10))
        .addRow(Longs(1, 11))
        .addRow(Longs(1, 12))
        .addRow(Longs(2, 20))
        .addRow(Longs(3, 30))
        .addRow(Longs(6, 50))
        .addRow(Longs(6, 51))
        .build

      row.moveToRow(inputPos)
      ArgumentStateMap.filterCancelledArguments(0,
                                                row,
                                                argumentRowId => argumentRowId % 2 == 0)

      row.getCurrentRow should be(outputPos)
      new ThenOutput(morsel, row, 2, 0)
        .shouldReturnRow(Longs(1, 10))
        .shouldReturnRow(Longs(1, 11))
        .shouldReturnRow(Longs(1, 12))
        .shouldReturnRow(Longs(3, 30))
        .shouldBeDone()
    }
  }

  class SumState(var sum: Long = 0){
    def keepGoing(add: Long): Boolean = {
      sum += add
      sum <= 32
    }
  }
}


