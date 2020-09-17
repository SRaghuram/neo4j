/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import org.eclipse.collections.api.set.MutableSet
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.SchedulingInputException
import org.neo4j.cypher.internal.runtime.pipelined.SchedulingInputException.AccumulatorAndMorselInput
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFactory
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator.ConcurrentHashTable
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator.HashTable
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator.StandardHashTable
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeLeftOuterHashJoinOperator.HashTableAndSet
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeLeftOuterHashJoinOperator.LeftOuterJoinFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.copyDataFromRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.ScopedMemoryTracker
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

class NodeLeftOuterHashJoinOperator(val workIdentity: WorkIdentity,
                                    lhsArgumentStateMapId: ArgumentStateMapId,
                                    rhsArgumentStateMapId: ArgumentStateMapId,
                                    lhsKeyOffsets: KeyOffsets,
                                    rhsKeyOffsets: KeyOffsets,
                                    rhsSlots: SlotConfiguration,
                                    rhsSlotMappings: SlotMappings)
                                   (val id: Id = Id.INVALID_ID) extends Operator {

  private val rhsLongMappings: Array[(Int, Int)] = rhsSlotMappings.longMappings
  private val rhsRefMappings: Array[(Int, Int)] = rhsSlotMappings.refMappings
  private val rhsCachedPropertyMappings: Array[(Int, Int)] = rhsSlotMappings.cachedPropertyMappings
  private val rhsOffsets: Array[Int] = rhsKeyOffsets.offsets
  private val rhsIsReference: Array[Boolean] = rhsKeyOffsets.isReference

  private val emptyRhsRowMorsel: Morsel = {
    val morsel = MorselFactory.allocate(rhsSlots, 1)
    val row = morsel.writeCursor(onFirstRow = true)
    rhsLongMappings.foreach { case (from, _) =>
      row.setLongAt(from, StatementConstants.NO_SUCH_ENTITY)
    }
    rhsRefMappings.foreach { case (from, _) =>
      row.setRefAt(from, Values.NO_VALUE)
    }
    row.next()
    row.truncate()
    morsel
  }

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(
      lhsArgumentStateMapId,
      new LeftOuterJoinFactory(lhsKeyOffsets, memoryTracker))
    argumentStateCreator.createArgumentStateMap(
      rhsArgumentStateMapId,
      new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id))
    new State
  }

  class State extends OperatorState {
    override def nextTasks(state: PipelinedQueryState,
                           operatorInput: OperatorInput,
                           parallelism: Int,
                           resources: QueryResources,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithDataAndAccumulator[Morsel, HashTableAndSet]] = {
      val input = operatorInput.takeAccumulatorAndData[Morsel, HashTableAndSet, MorselData]()
      if (input != null) {
        try {
          singletonIndexedSeq(new OTask(input.acc, input.payload))
        } catch {
          case NonFatalCypherError(t) =>
            throw SchedulingInputException(AccumulatorAndMorselInput(input), t)
        }
      } else {
        null
      }
    }
  }

  class OTask(override val accumulator: HashTableAndSet, morselData: MorselData)
    extends InputLoopWithMorselDataTask(morselData)
    with ContinuableOperatorTaskWithDataAndAccumulator[Morsel, HashTableAndSet] {

    private var lhsRows: LhsRows = new SimpleLhsRows(Collections.emptyIterator(), null)

    override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = ()

    override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = ()

    override def processRow(outputCursor: MorselWriteCursor,
                            inputCursor: MorselReadCursor): Unit = {
      val key = new Array[Long](rhsOffsets.length)
      fillKeyArray(inputCursor, key, rhsOffsets, rhsIsReference)

      if (NullChecker.entityIsNull(key(0))) {
        lhsRows = new SimpleLhsRows(Collections.emptyIterator(), null)
      } else {
        val keyValue = Values.longArray(key)
        lhsRows = new SimpleLhsRows(accumulator.hashTable.lhsRows(keyValue), inputCursor.snapshot())
        if (lhsRows.hasNext) {
          accumulator.addRhsKey(keyValue)
        }
      }

      writeToOutput(outputCursor)
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit = {
      // if we come back here as a continuation, make sure to only create missing LHS rows once
      morselData.argumentStream match {
        case EndOfEmptyStream | EndOfNonEmptyStream =>
          val neverSeenLhsKeys = accumulator.neverSeenLhsKeys
          emptyRhsRowMorsel.readCursor(onFirstRow = true)
          lhsRows = new ManyLhsRows(neverSeenLhsKeys, accumulator.hashTable)
        case _ =>
          lhsRows = new SimpleLhsRows(Collections.emptyIterator(), null)
      }
    }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit =
      writeToOutput(outputCursor)

    private def writeToOutput(outputCursor: MorselWriteCursor): Unit = {
      while (outputCursor.onValidRow && lhsRows.hasNext) {
        lhsRows.writeNext(outputCursor)
        outputCursor.next()
      }
    }

    override def canContinue: Boolean = super.canContinue || lhsRows.hasNext

    override def workIdentity: WorkIdentity = NodeLeftOuterHashJoinOperator.this.workIdentity

    override def toString: String = "NodeLeftOuterHashJoinTask"
  }

  trait LhsRows {
    def hasNext: Boolean
    def writeNext(outputRow: MorselWriteCursor): Unit
  }

  class SimpleLhsRows(inner: util.Iterator[Morsel], cursorToReadFrom: ReadableRow) extends LhsRows {
    override def hasNext: Boolean = inner.hasNext

    override def writeNext(outputRow: MorselWriteCursor): Unit = {
      val lhsRow = inner.next().readCursor(onFirstRow = true)
      copyDataFromRow(rhsLongMappings, rhsRefMappings, rhsCachedPropertyMappings, outputRow, cursorToReadFrom)
      outputRow.copyFrom(lhsRow)
    }
  }

  class ManyLhsRows(keys: util.Iterator[Value], hashTable: HashTable) extends LhsRows {
    private var lhsRows: java.util.Iterator[Morsel] = _
    private var current: Morsel = _
    private var _computeHasNext: Boolean = true

    override def hasNext: Boolean = {
      if (_computeHasNext) {
        current = computeNext()
        _computeHasNext = false
      }
      current != null
    }

    private def next(): Morsel = {
      if (!hasNext) {
        throw new IndexOutOfBoundsException
      }
      val next = current
      _computeHasNext = true
      next
    }

    override def writeNext(outputRow: MorselWriteCursor): Unit = {
      val lhsRow = next().readCursor(onFirstRow = true)
      copyDataFromRow(rhsLongMappings, rhsRefMappings, rhsCachedPropertyMappings, outputRow, emptyRhsRowMorsel.readCursor(onFirstRow = true))
      outputRow.copyFrom(lhsRow)
    }

    @scala.annotation.tailrec
    private def computeNext(): Morsel = {
      if (lhsRows != null && lhsRows.hasNext) {
        lhsRows.next()
      } else if (keys.hasNext) {
        lhsRows = hashTable.lhsRows(keys.next())
        computeNext()
      } else {
        null
      }
    }
  }
}

object NodeLeftOuterHashJoinOperator {
  class LeftOuterJoinFactory(lhsOffsets: KeyOffsets, memoryTracker: MemoryTracker) extends ArgumentStateFactory[HashTableAndSet] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): StandardHashTableAndSet =
      new StandardHashTableAndSet(new StandardHashTable(argumentRowId, lhsOffsets, argumentRowIdsForReducers, memoryTracker, acceptNulls = true), memoryTracker)
    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): ConcurrentHashTableAndSet = {
      new ConcurrentHashTableAndSet(new ConcurrentHashTable(argumentRowId, lhsOffsets, argumentRowIdsForReducers))
    }
  }

  trait HashTableAndSet extends MorselAccumulator[Morsel] {
    def hashTable: HashTable

    def addRhsKey(key: Value): Unit

    def neverSeenLhsKeys: java.util.Iterator[Value]
  }

  class StandardHashTableAndSet(val hashTable: HashTable, memoryTracker: MemoryTracker) extends HashTableAndSet {
    private val scopedMemoryTracker = new ScopedMemoryTracker(memoryTracker)
    private val rhsKeys: MutableSet[Value] = HeapTrackingCollections.newSet[Value](scopedMemoryTracker)

    override def update(data: Morsel, resources: QueryResources): Unit = hashTable.update(data, resources)

    override def argumentRowId: Long = hashTable.argumentRowId

    override def argumentRowIdsForReducers: Array[Long] = hashTable.argumentRowIdsForReducers

    override def addRhsKey(key: Value): Unit = rhsKeys.add(key)

    override def neverSeenLhsKeys: java.util.Iterator[Value] = {
      hashTable.keys.stream().filter(k => !rhsKeys.contains(k)).iterator()
    }

    override def close(): Unit = {
      scopedMemoryTracker.close()
    }
  }

  class ConcurrentHashTableAndSet(val hashTable: ConcurrentHashTable) extends HashTableAndSet {
    private val rhsKeys: java.util.Set[Value] = ConcurrentHashMap.newKeySet[Value]()

    override def update(data: Morsel, resources: QueryResources): Unit = hashTable.update(data, resources)

    override def argumentRowId: Long = hashTable.argumentRowId

    override def argumentRowIdsForReducers: Array[Long] = hashTable.argumentRowIdsForReducers

    override def addRhsKey(key: Value): Unit = rhsKeys.add(key)

    override def neverSeenLhsKeys: java.util.Iterator[Value] = {
      hashTable.keys.stream().filter(k => !rhsKeys.contains(k)).iterator()
    }

    override def close(): Unit = {
    }
  }
}

