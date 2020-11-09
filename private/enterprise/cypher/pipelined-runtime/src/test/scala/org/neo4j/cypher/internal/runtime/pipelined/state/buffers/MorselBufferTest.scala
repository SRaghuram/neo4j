/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselUnitTest
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

import scala.collection.mutable

/**
 * In tests for filterCancelledArguments we create morsels where columns correspond to argument row ids.
 * That means you can never have the same argument row id at a higher slot offset if the argument row id
 * at a lower slot offset changed - that would be an invalid morsel.
 *
 * We test with reducers and cancellers, that care about different slot offsets and one or multiple buffers
 * that are associates with all or some of the reducers and cancellers, that the correct rows get cancelled,
 * and that the reducers get correctly decremented.
 *
 * We expect a decrement if all rows for an argument row id at the reducers argument slot offset are cancelled.
 */
class MorselBufferTest extends MorselUnitTest with MorselTestHelper {

  private val ID = BufferId(1)
  private val tracker = mock[QueryCompletionTracker]

  test("filterCancelledArguments should decrement downstreamReduce when at same argumentOffset as workCanceller") {
    // Given
    val argumentSlotOffset = 1
    val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
    val cancellerMap = oddCancellerMap(argumentSlotOffset)
    val x = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMap.argumentStateMapId), _ => cancellerMap, null)

    // When
    val morsel =
      longMorsel(longsPerRow = 3)(
        //_, both, _
        1, 12, 11,
        1, 13, 12, // <cancelled>
        2, 13, 13, // <cancelled>
        2, 14, 14,
        3, 15, 15, // <cancelled>
        4, 16, 16
      )
    initiate(cancellerMap, 0 to 16)
    x.filterCancelledArguments(morsel)

    // Then
    reducer.assertDecrements(13, 15)
  }

  test("filterCancelledArguments should decrement downstreamReduce for argumentRowId=0") {
    // Given
    val argumentSlotOffset = 1
    val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
    val cancellerMap = evenCancellerMap(argumentSlotOffset)
    val x = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMap.argumentStateMapId), _ => cancellerMap, null)

    // When
    val morsel =
      longMorsel(longsPerRow = 3)(
        //_, both, _
        1, 0, 11, // <cancelled>
        1, 0, 12, // <cancelled>
        2, 0, 13, // <cancelled>
        2, 1, 14,
        3, 2, 15, // <cancelled>
        4, 3, 16
      )
    initiate(cancellerMap, 0 to 3)
    x.filterCancelledArguments(morsel)

    // Then
    reducer.assertDecrements(0, 2)
  }

  test("filterCancelledArguments should decrement downstreamReduce when at lower argumentOffset as workCanceller") {
    // Given
    val reducerArgumentSlotOffset = 2
    val reducer = new TestAccumulatingBuffer(reducerArgumentSlotOffset)
    val cancellerArgumentSlotOffset = 1
    val cancellerMap = oddCancellerMap(cancellerArgumentSlotOffset)
    val x = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMap.argumentStateMapId), _ => cancellerMap, null)

    // When
    val morsel =
      longMorsel(longsPerRow = 3)(
        //_, canceller, reducer
        1, 12, 21,
        1, 13, 22, // <cancelled>
        2, 13, 23, // <cancelled>
        2, 13, 23, // <cancelled>
        2, 14, 24,
        3, 15, 26, // <cancelled>
        4, 16, 27
      )
    initiate(cancellerMap, 0 to 16)
    x.filterCancelledArguments(morsel)

    // Then
    reducer.assertDecrements(22, 23, 26)
  }

  test("filterCancelledArguments should decrement downstreamReduce when at higher argumentOffset as workCanceller") {
    // Given
    val reducerArgumentSlotOffset = 1
    val reducer = new TestAccumulatingBuffer(reducerArgumentSlotOffset)
    val cancellerArgumentSlotOffset = 2
    val cancellerMap = oddCancellerMap(cancellerArgumentSlotOffset)
    val x = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMap.argumentStateMapId), _ => cancellerMap, null)

    // When
    val morsel =
      longMorsel(longsPerRow = 3)(
        //_, reducer, canceller
        1, 11, 21, // <cancelled>
        1, 11, 22,
        2, 12, 23, // <cancelled>
        2, 12, 25, // <cancelled>
        3, 13, 26,
        3, 13, 28
      )
    initiate(cancellerMap, 0 to 28)
    x.filterCancelledArguments(morsel)

    // Then
    reducer.assertDecrements(12)
  }

  test("two cancellers, and two reducers") {
    // Given
    val firstArgumentSlotOffset = 0
    val reducer1 = new TestAccumulatingBuffer(firstArgumentSlotOffset)
    val secondArgumentSlotOffset = 1
    val reducer2 = new TestAccumulatingBuffer(secondArgumentSlotOffset)

    def createMorsel = longMorsel(longsPerRow = 2)(
      // reducer1&canceller1, reducer2&canceller2
      0, 0,
      0, 1, // <cancelled by c2>
      1, 2, // <cancelled by c1>
      1, 3, // <cancelled by c1>
      2, 4, // <cancelled by c1 && c2>
      2, 5, // <cancelled by c1>
      3, 6, // <cancelled by c2>
      3, 7, // <cancelled by c2>
      4, 8,
      4, 9
    )
    def createCancellerMaps = {
      val cancellerMaps = Array(
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), firstArgumentSlotOffset, CancellerFactory(Seq(1, 2).contains), EmptyMemoryTracker.INSTANCE),
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(1), secondArgumentSlotOffset, CancellerFactory(Seq(1, 4, 6, 7).contains), EmptyMemoryTracker.INSTANCE))
      initiate(cancellerMaps(0), 0 to 4)
      initiate(cancellerMaps(1), 0 to 9)
      cancellerMaps
    }

    // When with one buffer
    {
      val morsel = createMorsel
      val cancellerMaps = createCancellerMaps
      val singleBuffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2), ReadOnlyArray(cancellerMaps.map(_.argumentStateMapId):_*), id => cancellerMaps(id.x), null)

      singleBuffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(1, 2, 3)
      reducer2.assertDecrements(1, 2, 3, 4, 5, 6, 7)
    }

    // When with first c1 then c2 buffer
    {
      val morsel = createMorsel
      val cancellerMaps = createCancellerMaps
      val c1Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2), ReadOnlyArray(cancellerMaps(0).argumentStateMapId), _ => cancellerMaps(0), null)
      val c2Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2), ReadOnlyArray(cancellerMaps(1).argumentStateMapId), _ => cancellerMaps(1), null)

      c1Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(1, 2)
      reducer2.assertDecrements(2, 3, 4, 5)

      c2Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(3)
      reducer2.assertDecrements(1, 6, 7)
    }

    // When with first c2 then c1 buffer
    {
      val morsel = createMorsel
      val cancellerMaps = createCancellerMaps
      val c1Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2), ReadOnlyArray(cancellerMaps(0).argumentStateMapId), _ => cancellerMaps(0), null)
      val c2Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2), ReadOnlyArray(cancellerMaps(1).argumentStateMapId), _ => cancellerMaps(1), null)

      c2Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(3)
      reducer2.assertDecrements(1, 4, 6, 7)

      c1Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(1, 2)
      reducer2.assertDecrements(2, 3, 5)
    }
  }

  test("two cancellers, and three reducers") {
    // Given
    val firstArgumentSlotOffset = 0
    val reducer1 = new TestAccumulatingBuffer(firstArgumentSlotOffset)
    val secondArgumentSlotOffset = 1
    val reducer2 = new TestAccumulatingBuffer(secondArgumentSlotOffset)
    val thirdArgumentSlotOffset = 2
    val reducer3 = new TestAccumulatingBuffer(thirdArgumentSlotOffset)

    // When
    def createMorsel =
      longMorsel(longsPerRow = 3)(
        // reducer1, reducer2&canceller2, reducer3&canceller1
        0, 1, 1, // <cancelled by c1>
        0, 1, 2, // <cancelled by c1>
        0, 2, 3,
        1, 3, 4, // <cancelled by c2>
        1, 3, 5, // <cancelled by c2 && c1>
        1, 3, 5, // <cancelled by c2 && c1>
        1, 4, 6, // <cancelled by c1>
        1, 5, 7, // <cancelled by c2 && c1>
        2, 6, 8,
        2, 7, 9,  // <cancelled by c2>
        2, 7, 10,  // <cancelled by c2>
        3, 8, 11,
        3, 9, 12,
        4, 10, 13, // <cancelled by c2 && c1>
        4, 10, 14, // <cancelled by c2>
        4, 10, 15  // <cancelled by c2>
      )
    def createCancellerMaps = {
      val cancellerMaps = Array(
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), thirdArgumentSlotOffset, CancellerFactory(Seq(1, 2, 5, 6, 7, 13).contains), EmptyMemoryTracker.INSTANCE),
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(1), secondArgumentSlotOffset, CancellerFactory(Seq(3, 5, 7, 10).contains), EmptyMemoryTracker.INSTANCE))
      initiate(cancellerMaps(0), 0 to 15)
      initiate(cancellerMaps(1), 0 to 10)
      cancellerMaps
    }


    // When with one buffer
    {
      val morsel = createMorsel
      val cancellerMaps = createCancellerMaps
      val singleBuffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2, reducer3), ReadOnlyArray(cancellerMaps.map(_.argumentStateMapId):_*), id => cancellerMaps(id.x), null)

      singleBuffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(1, 4)
      reducer2.assertDecrements(1, 3, 4, 5, 7, 10)
      reducer3.assertDecrements(1, 2, 4, 5, 6, 7, 9, 10, 13, 14, 15)
    }

    // When with first c1 then c2 buffer
    {
      val morsel = createMorsel
      val cancellerMaps = createCancellerMaps
      val c1Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2, reducer3), ReadOnlyArray(cancellerMaps(0).argumentStateMapId), _ => cancellerMaps(0), null)
      val c2Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2, reducer3), ReadOnlyArray(cancellerMaps(1).argumentStateMapId), _ => cancellerMaps(1), null)

      c1Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements()
      reducer2.assertDecrements(1, 4, 5)
      reducer3.assertDecrements(1, 2, 5, 6, 7, 13)

      c2Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(1, 4)
      reducer2.assertDecrements(3, 7, 10)
      reducer3.assertDecrements(4, 9, 10, 14, 15)
    }

    // When with first c2 then c1 buffer
    {
      val morsel = createMorsel
      val cancellerMaps = createCancellerMaps
      val c1Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2, reducer3), ReadOnlyArray(cancellerMaps(0).argumentStateMapId), _ => cancellerMaps(0), null)
      val c2Buffer = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2, reducer3), ReadOnlyArray(cancellerMaps(1).argumentStateMapId), _ => cancellerMaps(1), null)

      c2Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(4)
      reducer2.assertDecrements(3, 5, 7, 10)
      reducer3.assertDecrements(4, 5, 7, 9, 10, 13, 14, 15)

      c1Buffer.filterCancelledArguments(morsel)
      reducer1.assertDecrements(1)
      reducer2.assertDecrements(1, 4)
      reducer3.assertDecrements(1, 2, 6)
    }
  }

  test("empty morsel and two reducers") {
    // Given
    val firstArgumentSlotOffset = 0
    val reducer1 = new TestAccumulatingBuffer(firstArgumentSlotOffset)
    val secondArgumentSlotOffset = 1
    val reducer2 = new TestAccumulatingBuffer(secondArgumentSlotOffset)

    val cancellerMaps = Array(
      new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), firstArgumentSlotOffset, CancellerFactory(Seq(1, 2).contains), EmptyMemoryTracker.INSTANCE),
      new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(1), secondArgumentSlotOffset, CancellerFactory(Seq(1, 4, 6, 7).contains), EmptyMemoryTracker.INSTANCE))

    val x = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer1, reducer2), ReadOnlyArray(cancellerMaps.map(_.argumentStateMapId):_*), id => cancellerMaps(id.x), null)

    // When
    val morsel =
      longMorsel(longsPerRow = 2)()
    x.filterCancelledArguments(morsel)

    // Then
    reducer1.assertDecrements()
    reducer2.assertDecrements()
  }

  // Restore currentPos
  for (inputPos <- Seq(0, 3, 4, 5, 6, 7)) {

    test(s"filterCancelledArguments should work with current row $inputPos") {

      // Given
      val argumentSlotOffset = 0
      val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
      val cancellerMap = evenCancellerMap(argumentSlotOffset)
      val x = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMap.argumentStateMapId), _ => cancellerMap, null)

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

      initiate(cancellerMap, 0 to 6)
      x.filterCancelledArguments(morsel)

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
    (name, cancelledByC1, cancelledByC2) <- Seq(
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
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(cancelledByC1), EmptyMemoryTracker.INSTANCE),
        new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(1), argumentSlotOffset, CancellerFactory(cancelledByC2), EmptyMemoryTracker.INSTANCE))

      val x = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMaps.map(_.argumentStateMapId):_*), id => cancellerMaps(id.x), null)

      // When
      val morsel = buildSequentialInput(numberOfRows)
      initiate(cancellerMaps(0), 0 to numberOfRows)
      initiate(cancellerMaps(1), 0 to numberOfRows)
      x.filterCancelledArguments(morsel)

      // Then
      validateRows(morsel, numberOfRows, x => !cancelledByC1(x) && !cancelledByC2(x))
    }

    test(s"filterCancelledArguments with two buffers - $name - $numberOfRows rows") {
      // Given
      val argumentSlotOffset = 0
      val reducer = new TestAccumulatingBuffer(argumentSlotOffset)
      val cancellerMap1 = new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(cancelledByC1), EmptyMemoryTracker.INSTANCE)
      val x1 = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMap1.argumentStateMapId), _ => cancellerMap1, null)

      val cancellerMap2 = new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(cancelledByC2), EmptyMemoryTracker.INSTANCE)
      val x2 = new MorselBuffer(ID, tracker, ReadOnlyArray(reducer), ReadOnlyArray(cancellerMap2.argumentStateMapId), _ => cancellerMap2, null)

      // When
      val morsel = buildSequentialInput(numberOfRows)
      initiate(cancellerMap1, 0 to numberOfRows)
      initiate(cancellerMap2, 0 to numberOfRows)

      x1.filterCancelledArguments(morsel)
      x2.filterCancelledArguments(morsel)

      // Then
      validateRows(morsel, numberOfRows, x => !cancelledByC1(x) && !cancelledByC2(x))
    }
  }

  // HELPERS

  private def initiate(asm: ArgumentStateMap[_], argumentRowIds: Range): Unit = {
    for (argId <- argumentRowIds) {
      asm.initiate(argId, null, Array.empty, 1)
    }
  }

  private def oddCancellerMap(argumentSlotOffset: Int): ArgumentStateMap[StaticCanceller] =
    new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(_ % 2 == 1), EmptyMemoryTracker.INSTANCE)

  private def evenCancellerMap(argumentSlotOffset: Int): ArgumentStateMap[StaticCanceller] =
    new StandardArgumentStateMap[StaticCanceller](ArgumentStateMapId(0), argumentSlotOffset, CancellerFactory(_ % 2 == 0), EmptyMemoryTracker.INSTANCE)

  class TestAccumulatingBuffer(override val argumentSlotOffset: Int) extends Buffers.AccumulatingBuffer {

    val initiates = new mutable.HashMap[Long, Int]()
    val increments = new mutable.HashMap[Long, Int]()
    val decrements = new mutable.HashMap[Long, Int]()

    def assertDecrements(expected: Int*): Unit = {
      initiates shouldBe Map.empty
      increments shouldBe Map.empty
      decrements shouldBe expected.map((_, 1)).toMap
      reset()
    }

    def reset(): Unit = {
      initiates.clear()
      increments.clear()
      decrements.clear()
    }

    override def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit =
      initiates(argumentRowId) = initiates.getOrElseUpdate(argumentRowId, 0) + 1

    override def increment(argumentRowId: Long): Unit =
      increments(argumentRowId) = increments.getOrElseUpdate(argumentRowId, 0) + 1

    override def decrement(argumentRowId: Long): Unit =
      decrements(argumentRowId) = decrements.getOrElseUpdate(argumentRowId, 0) + 1
  }

  class StaticCanceller(override val isCancelled: Boolean, val argumentRowId: Long, memoryTracker: MemoryTracker) extends WorkCanceller {
    memoryTracker.allocateHeap(StaticCanceller.SHALLOW_SIZE)
    override def argumentRowIdsForReducers: Array[Long] = ???

    override def remaining: Long = if (isCancelled) 0L else Long.MaxValue

    override def close(): Unit = {
      memoryTracker.releaseHeap(StaticCanceller.SHALLOW_SIZE)
      super.close()
    }

    override def shallowSize: Long = StaticCanceller.SHALLOW_SIZE
  }

  object StaticCanceller {
    private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[StaticCanceller])
  }

  case class CancellerFactory(predicate: Long => Boolean) extends ArgumentStateMap.ArgumentStateFactory[StaticCanceller] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): StaticCanceller =
      new StaticCanceller(predicate(argumentRowId), argumentRowId, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): StaticCanceller = ???
  }

}
