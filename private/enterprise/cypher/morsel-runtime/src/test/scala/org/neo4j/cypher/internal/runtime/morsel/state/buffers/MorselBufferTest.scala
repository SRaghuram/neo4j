/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.morsel.execution.{FilteringMorselExecutionContext, Morsel, MorselExecutionContext}
import org.neo4j.cypher.internal.runtime.morsel.operators.MorselUnitTest
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, QueryCompletionTracker, StandardArgumentStateMap}
import org.neo4j.cypher.internal.v4_0.util.symbols

import scala.collection.mutable

class MorselBufferTest extends MorselUnitTest {

  private val ID = BufferId(1)
  private val tracker = mock[QueryCompletionTracker]

  test("filterCancelledArguments should decrement downstreamReduce when at same argumentOffset as workCanceller") {

    // Given
    val argumentSlotOffset = 1
    val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
    val cancellerMap = oddCancellerMap(argumentSlotOffset)
    val x = new MorselBuffer(ID, tracker, Array(reducer), Array(cancellerMap.argumentStateMapId), id => cancellerMap, null)

    // When
    val morsel =
      longMorsel(longsPerRow = 3)(
        //_, both, _
        1, 12, 11,
        1, 13, 12,
        2, 13, 13,
        2, 14, 14,
        3, 15, 15,
        4, 16, 16
      )
    initiate(cancellerMap, 1 to 16)
    x.filterCancelledArguments(morsel)

    // Then
    reducer.initiates shouldBe Map.empty
    reducer.increments shouldBe Map.empty
    reducer.decrements shouldBe Map(13 -> 1, 15 -> 1)
  }

  test("filterCancelledArguments should decrement downstreamReduce for argumentRowId=0") {

    // Given
    val argumentSlotOffset = 1
    val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
    val cancellerMap = evenCancellerMap(argumentSlotOffset)
    val x = new MorselBuffer(ID, tracker, Array(reducer), Array(cancellerMap.argumentStateMapId), id => cancellerMap, null)

    // When
    val morsel =
      longMorsel(longsPerRow = 3)(
        //_, both, _
        1, 0, 11,
        1, 0, 12,
        2, 0, 13,
        2, 1, 14,
        3, 2, 15,
        4, 3, 16
      )
    initiate(cancellerMap, 0 to 16)
    x.filterCancelledArguments(morsel)

    // Then
    reducer.initiates shouldBe Map.empty
    reducer.increments shouldBe Map.empty
    reducer.decrements shouldBe Map(0 -> 1, 2 -> 1)
  }

  test("filterCancelledArguments should decrement downstreamReduce when at different argumentOffset as workCanceller") {

    // Given
    val reducerArgumentSlotOffset = 2
    val reducer = new TestAccumulatingBuffer(reducerArgumentSlotOffset)
    val cancellerArgumentSlotOffset = 1
    val cancellerMap = oddCancellerMap(cancellerArgumentSlotOffset)
    val x = new MorselBuffer(ID, tracker, Array(reducer), Array(cancellerMap.argumentStateMapId), id => cancellerMap, null)

    // When
    val morsel =
      longMorsel(longsPerRow = 3)(
        //_, canceller, reducer
        1, 12, 21,
        1, 13, 22,
        2, 13, 23,
        2, 13, 23,
        2, 14, 23,
        3, 15, 26,
        4, 16, 26
      )
    initiate(cancellerMap, 1 to 26)
    x.filterCancelledArguments(morsel)

    // Then
    reducer.initiates shouldBe Map.empty
    reducer.increments shouldBe Map.empty
    reducer.decrements shouldBe Map(22 -> 1, 23 -> 1, 26 -> 1)
  }

  // Restore currentPos
  for (inputPos <- Seq(0, 3, 4, 5, 6, 7)) {

    test(s"filterCancelledArguments should work with current row $inputPos") {

      // Given
      val argumentSlotOffset = 0
      val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
      val cancellerMap = evenCancellerMap(argumentSlotOffset)
      val x = new MorselBuffer(ID, tracker, Array(reducer), Array(cancellerMap.argumentStateMapId), id => cancellerMap, null)

      // When
      val morsel =
        new FilteringInput()
          .addRow(Longs(1, 10))
          .addRow(Longs(1, 11))
          .addRow(Longs(1, 12))
          .addRow(Longs(2, 20))
          .addRow(Longs(3, 30))
          .addRow(Longs(6, 50))
          .addRow(Longs(6, 51))
          .build()
      morsel.setCurrentRow(inputPos)

      initiate(cancellerMap, 1 to 6)
      x.filterCancelledArguments(morsel)

      morsel.getCurrentRow should be(inputPos)
      new ThenOutput(morsel, 2, 0)
        .shouldReturnRow(Longs(1, 10))
        .shouldReturnRow(Longs(1, 11))
        .shouldReturnRow(Longs(1, 12))
        .shouldReturnRow(Longs(3, 30))
        .shouldBeDone()
    }
  }

  // Two cancellers
  for {
    numberOfRows <- Seq(0, 1, 2, 3, 5, 8, 13, 21)
    (name, p1, p2) <- Seq(
      ("first half -> modulo 2",       ltPredicate(numberOfRows / 2),  moduloPredicate(2)),
      ("first half -> modulo 3",       ltPredicate(numberOfRows / 2),  moduloPredicate(3)),
      ("first half -> first half",     ltPredicate(numberOfRows / 2),  ltPredicate(numberOfRows / 4)),
      ("second half -> modulo 2",      gtePredicate(numberOfRows / 2), moduloPredicate(2)),
      ("second half -> modulo 3",      gtePredicate(numberOfRows / 2), moduloPredicate(2)),
      ("second half -> second half",   gtePredicate(numberOfRows / 2), gtePredicate(numberOfRows - (numberOfRows / 4))),
      ("always false -> always false", alwaysFalsePredicate,           alwaysFalsePredicate),
      ("always false -> always true",  alwaysFalsePredicate,           alwaysTruePredicate),
      ("always true -> always true",   alwaysTruePredicate,            alwaysTruePredicate),
      ("always true -> always false",  alwaysTruePredicate,            alwaysFalsePredicate)
    )
  } {
    test(s"filterCancelledArguments with two cancellers - $name - $numberOfRows rows") {
      // Given
      val argumentSlotOffset = 0
      val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
      val cancellerMaps = Array(
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(p1)),
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(1), argumentSlotOffset, CancellerFactory(p2)))

      val x = new MorselBuffer(ID, tracker, Array(reducer), cancellerMaps.map(_.argumentStateMapId), id => cancellerMaps(id.x), null)

      // When
      val morsel = buildSequentialInput(numberOfRows)
      initiate(cancellerMaps(0), 0 to numberOfRows)
      initiate(cancellerMaps(1), 0 to numberOfRows)
      x.filterCancelledArguments(morsel)

      // Then
      validateRows(morsel, numberOfRows, x => p1(x) && p2(x))
    }

    test(s"filterCancelledArguments with two buffers - $name - $numberOfRows rows") {
      // Given
      val argumentSlotOffset = 0
      val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
      val cancellerMap1 = new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(p1))
      val x1 = new MorselBuffer(ID, tracker, Array(reducer), Array(cancellerMap1.argumentStateMapId), id => cancellerMap1, null)

      val cancellerMap2 = new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(p2))
      val x2 = new MorselBuffer(ID, tracker, Array(reducer), Array(cancellerMap2.argumentStateMapId), id => cancellerMap2, null)

      // When
      val morsel = buildSequentialInput(numberOfRows)
      initiate(cancellerMap1, 0 to numberOfRows)
      initiate(cancellerMap2, 0 to numberOfRows)

      x1.filterCancelledArguments(morsel)
      x2.filterCancelledArguments(morsel)

      // Then
      validateRows(morsel, numberOfRows, x => p1(x) && p2(x))
    }
  }

  // HELPERS

  private def longMorsel(longsPerRow: Int)(values: Long*): MorselExecutionContext = {
    val nRows = values.size / longsPerRow
    var slots =
      (0 until longsPerRow)
        .foldLeft(SlotConfiguration.empty)( (slots, i) => slots.newLong(s"v$i", nullable = false, symbols.CTAny) )

    new FilteringMorselExecutionContext(new Morsel(values.toArray, Array.empty), slots, nRows, 0, 0, nRows)
  }

  private def initiate(asm: ArgumentStateMap[_], argumentRowIds: Range): Unit = {
    for (argId <- argumentRowIds) {
      asm.initiate(argId, null, Array.empty)
    }
  }

  private def oddCancellerMap(argumentSlotOffset: Int): ArgumentStateMap[StaticCanceller] =
    new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(_ % 2 == 0))

  private def evenCancellerMap(argumentSlotOffset: Int): ArgumentStateMap[StaticCanceller] =
    new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(_ % 2 == 1))

  class TestAccumulatingBuffer(override val argumentSlotOffset: Int) extends Buffers.AccumulatingBuffer {

    val initiates = new mutable.HashMap[Long, Int]()
    val increments = new mutable.HashMap[Long, Int]()
    val decrements = new mutable.HashMap[Long, Int]()

    override def initiate(argumentRowId: Long, argumentMorsel: MorselExecutionContext): Unit =
      initiates(argumentRowId) = initiates.getOrElseUpdate(argumentRowId, 0) + 1

    override def increment(argumentRowId: Long): Unit =
      increments(argumentRowId) = increments.getOrElseUpdate(argumentRowId, 0) + 1

    override def decrement(argumentRowId: Long): Unit =
      decrements(argumentRowId) = decrements.getOrElseUpdate(argumentRowId, 0) + 1
  }

  class StaticCanceller(override val isCancelled: Boolean, val argumentRowId: Long) extends WorkCanceller {
    override def argumentRowIdsForReducers: Array[Long] = ???
  }

  case class CancellerFactory(predicate: Long => Boolean) extends ArgumentStateMap.ArgumentStateFactory[StaticCanceller] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselExecutionContext,
                                          argumentRowIdsForReducers: Array[Long]): StaticCanceller =
      new StaticCanceller(!predicate(argumentRowId), argumentRowId)

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselExecutionContext,
                                            argumentRowIdsForReducers: Array[Long]): StaticCanceller = ???
  }

}
