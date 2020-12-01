/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.github.jamm.MemoryMeter
import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselUnitTest
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ArgumentStateMapMemoryManagementTest extends MorselUnitTest {

  private val SIZE_HINT = 100

  private val meter = new MemoryMeter
  private val memoryTracker = new LocalMemoryTracker
  private val measuredMemoryTracker = meter.measureDeep(memoryTracker)

  override def afterEach(): Unit = {
    super.afterEach()
    memoryTracker.reset()
  }

  test(s"memory allocation initiate - update - clear") {
    // Given
    val numberOfArguments = SIZE_HINT
    val (asm, morsel, excluded) = createTestAsmAndMorsel(numberOfArguments)

    // Then
    val measuredFirst = assertMemoryEstimationExact(asm, excluded, 0)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.initiate(arg, view.readCursor(), null, 1)
    })

    assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.update(arg, (state: TestArgumentState) => {
        state.put(Values.longValue(arg))
      })
    })

    val measuredBeforeClear = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    asm.clearAll((state: TestArgumentState) => {
      state.close()
    })

    val measuredAfterClear = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    measuredBeforeClear should be > measuredAfterClear
  }

  test(s"memory allocation initiate - takeCompleted") {
    // Given
    val numberOfArguments = SIZE_HINT
    val (asm, morsel, excluded) = createTestAsmAndMorsel(numberOfArguments)

    // Then
    val measuredFirst = assertMemoryEstimationExact(asm, excluded, 0)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.initiate(arg, view.readCursor(), null, 1)
    })

    assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.update(arg, (state: TestArgumentState) => {
        state.put(Values.longValue(arg))
      })
    })

    val measuredFull = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    // Complete and takeCompleted in two steps
    Random.setSeed(42L)
    val argRange: Seq[Int] = 0 until numberOfArguments
    val args = Random.shuffle(argRange)
    val (args1, args2) = args.splitAt(args.size / 2)

    // Phase 1
    args1.foreach { arg =>
      val state = asm.decrement(arg)
      state should not be null
    }
    val taken1 = asm.takeCompleted(args1.size)
    taken1.size shouldEqual args1.size
    taken1.foreach(_.close())

    val measuredHalfFull = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    // Phase 2
    args2.foreach { arg =>
      val state = asm.decrement(arg)
      state should not be null
    }
    val taken2 = asm.takeCompleted(args2.size)
    taken2.size shouldEqual args2.size
    taken2.foreach(_.close())

    assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    val measuredEmpty = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    measuredFull should be > measuredHalfFull
    measuredHalfFull should be > measuredEmpty
  }

  test(s"memory allocation initiate - takeIfCompletedOrElsePeek") {
    // Given
    val numberOfArguments = SIZE_HINT
    val (asm, morsel, excluded) = createTestAsmAndMorsel(numberOfArguments)

    // Then
    val measuredFirst = assertMemoryEstimationExact(asm, excluded, 0)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.initiate(arg, view.readCursor(), null, 1)
    })

    assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.update(arg, (state: TestArgumentState) => {
        state.put(Values.longValue(arg))
      })
    })

    val measuredFull = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    // Complete and takeCompleted in two steps
    Random.setSeed(666L)
    val argRange: Seq[Int] = 0 until numberOfArguments
    val args = Random.shuffle(argRange)
    val (args1, args2) = args.splitAt(args.size / 2)

    // Phase 1
    val taken1 = new ArrayBuffer[TestArgumentState]()
    args1.foreach { arg =>
      val peeked: ArgumentStateMap.ArgumentStateWithCompleted[TestArgumentState] = asm.takeIfCompletedOrElsePeek(arg)
      peeked should not be null
      peeked.isCompleted shouldBe false

      val state = asm.decrement(arg)
      state should not be null

      val taken = asm.takeIfCompletedOrElsePeek(arg)
      taken should not be null
      taken.isCompleted shouldBe true

      peeked.argumentState should be theSameInstanceAs state
      taken.argumentState should be theSameInstanceAs state

      taken1 += taken.argumentState
    }
    taken1.foreach(_.close())

    val measuredHalfFull = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    // Phase 2
    val taken2 = new ArrayBuffer[TestArgumentState]()
    args2.foreach { arg =>
      val peeked: ArgumentStateMap.ArgumentStateWithCompleted[TestArgumentState] = asm.takeIfCompletedOrElsePeek(arg)
      peeked should not be null
      peeked.isCompleted shouldBe false

      val state = asm.decrement(arg)
      state should not be null

      val taken = asm.takeIfCompletedOrElsePeek(arg)
      taken should not be null
      taken.isCompleted shouldBe true

      peeked.argumentState should be theSameInstanceAs state
      taken.argumentState should be theSameInstanceAs state

      taken2 += taken.argumentState
    }
    taken2.foreach(_.close())

    val measuredEmpty = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    measuredFull should be > measuredHalfFull
    measuredHalfFull should be > measuredEmpty
  }

  test(s"memory allocation initiate - takeOneIfCompletedOrElsePeek") {
    // Given
    val numberOfArguments = SIZE_HINT
    val (asm, morsel, excluded) = createTestAsmAndMorsel(numberOfArguments)

    // Then
    val measuredFirst = assertMemoryEstimationExact(asm, excluded, 0)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.initiate(arg, view.readCursor(), null, 1)
    })

    assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    ArgumentStateMap.foreach(0, morsel, (arg: Long, view: Morsel) => {
      asm.update(arg, (state: TestArgumentState) => {
        state.put(Values.longValue(arg))
      })
    })

    val measuredFull = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    // Complete and takeCompleted in two steps
    val args = 0 until numberOfArguments
    val (args1, args2) = args.splitAt(args.size / 2)

    // Phase 1
    val taken1 = new ArrayBuffer[TestArgumentState]()
    args1.foreach { arg =>
      val peeked: ArgumentStateMap.ArgumentStateWithCompleted[TestArgumentState] = asm.takeOneIfCompletedOrElsePeek()
      peeked should not be null
      peeked.isCompleted shouldBe false

      val state = asm.decrement(arg)
      state should not be null

      val taken = asm.takeOneIfCompletedOrElsePeek()
      taken should not be null
      taken.isCompleted shouldBe true

      peeked.argumentState should be theSameInstanceAs state
      taken.argumentState should be theSameInstanceAs state

      taken1 += taken.argumentState
    }
    taken1.foreach(_.close())

    val measuredHalfFull = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    // Phase 2
    val taken2 = new ArrayBuffer[TestArgumentState]()
    args2.foreach { arg =>
      val peeked: ArgumentStateMap.ArgumentStateWithCompleted[TestArgumentState] = asm.takeOneIfCompletedOrElsePeek()
      peeked should not be null
      peeked.isCompleted shouldBe false

      val state = asm.decrement(arg)
      state should not be null

      val taken = asm.takeOneIfCompletedOrElsePeek()
      taken should not be null
      taken.isCompleted shouldBe true

      peeked.argumentState should be theSameInstanceAs state
      taken.argumentState should be theSameInstanceAs state

      taken2 += taken.argumentState
    }
    taken2.foreach(_.close())

    val measuredEmpty = assertMemoryEstimationExact(asm, excluded, numberOfArguments)

    measuredFull should be > measuredHalfFull
    measuredHalfFull should be > measuredEmpty
  }

  private def assertMemoryEstimationExact(asm: ArgumentStateMap[TestArgumentState], excluded: Long, numberOfArguments: Long): Long = {
    val measured = meter.measureDeep(asm) - excluded
    val estimated = memoryTracker.estimatedHeapMemory()
    val overestimation = estimated - measured
    val estimationError = if (overestimation > 0) "Overestimated" else "Underestimated"
    val diff = Math.abs(overestimation)
    val clue =
      if (numberOfArguments > 0)
        s"$estimationError by $diff bytes, or ${diff / numberOfArguments} bytes per argument\n"
      else
        s"$estimationError by $diff bytes\n"
    withClue(clue) {
      measured shouldEqual estimated
    }
    measured
  }

  private def assertNoMemoryLeak(): Unit = {
    withClue("Leaking memory") {
      0L shouldEqual memoryTracker.estimatedHeapMemory
    }
  }

  private def createTestAsmAndMorsel(numberOfArguments: Int): (ArgumentStateMap[TestArgumentState], Morsel, Long) = {
    // Build a morsel with numberOfArgument rows and one unique argument id per row in slot offset 0
    val morsel = buildSequentialInput(numberOfArguments)
    val stateFactory = new StandardStateFactory
    val argumentStateFactory = TestArgumentStateFactory
    val asmId = ArgumentStateMapId(0)
    val asm = stateFactory.newArgumentStateMap(asmId, 0, argumentStateFactory, orderPreservingInParallel = false, memoryTracker, morselSize = numberOfArguments)

    // Calculate bytes to be excluded from real measurement
    val measuredAsmId = meter.measureDeep(asmId)
    //val measuredAsmFactory = meter.measureDeep(argumentStateFactory) // TODO: Why is this not needed?
    val excluded = measuredMemoryTracker + measuredAsmId

    (asm, morsel, excluded)
  }
}

object TestArgumentStateFactory extends ArgumentStateFactory[TestArgumentState] {
  override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker): TestArgumentState = {
    val scopedMemoryTracker = memoryTracker.getScopedMemoryTracker
    scopedMemoryTracker.allocateHeap(SCOPED_MEMORY_TRACKER_SHALLOW_SIZE)
    new TestArgumentState(argumentRowId, scopedMemoryTracker)
  }
  override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): TestArgumentState = ???
}

class TestArgumentState(val argumentRowId: Long, val scopedMemoryTracker: MemoryTracker) extends ArgumentState {
  private val list: HeapTrackingArrayList[AnyValue] = HeapTrackingArrayList.newArrayList(scopedMemoryTracker)

  val l1: Long = 1L
  val l2: Long = 2L
  val l3: Long = 3L

  //println(s"TestArgumentState $argumentRowId mem=${scopedMemoryTracker.estimatedHeapMemory()}")

  def put(value: AnyValue): Unit = {
    scopedMemoryTracker.allocateHeap(value.estimatedHeapUsage())
    list.add(value)
  }

  override def close(): Unit = {
    //println(s"TestArgumentState CLOSE $argumentRowId mem=${scopedMemoryTracker.estimatedHeapMemory()}")
    scopedMemoryTracker.close()
    super.close()
  }

  override def shallowSize: Long = TestArgumentState.SHALLOW_SIZE

  override def argumentRowIdsForReducers: Array[Long] = Array.empty[Long]

  override def toString: String = {
    s"State($argumentRowId)"
  }
}

object TestArgumentState {
  val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[TestArgumentState])
}

