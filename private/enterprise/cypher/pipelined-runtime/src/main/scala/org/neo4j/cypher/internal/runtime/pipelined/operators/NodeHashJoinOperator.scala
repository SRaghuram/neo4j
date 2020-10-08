/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator.HashTable
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator.HashTableFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.copyDataFromRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

class NodeHashJoinOperator(val workIdentity: WorkIdentity,
                           lhsArgumentStateMapId: ArgumentStateMapId,
                           rhsArgumentStateMapId: ArgumentStateMapId,
                           lhsKeyOffsets: KeyOffsets,
                           rhsKeyOffsets: KeyOffsets,
                           rhsSlotMappings: SlotMappings)
                          (val id: Id = Id.INVALID_ID) extends Operator with AccumulatorsAndMorselInputOperatorState[Morsel, HashTable, Morsel] {

  private val rhsLongMappings: Array[(Int, Int)] = rhsSlotMappings.longMappings
  private val rhsRefMappings: Array[(Int, Int)] = rhsSlotMappings.refMappings
  private val rhsCachedPropertyMappings: Array[(Int, Int)] = rhsSlotMappings.cachedPropertyMappings
  private val rhsOffsets: Array[Int] = rhsKeyOffsets.offsets
  private val rhsIsReference: Array[Boolean] = rhsKeyOffsets.isReference

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(
      lhsArgumentStateMapId,
      new HashTableFactory(lhsKeyOffsets, memoryTracker), memoryTracker)
    argumentStateCreator.createArgumentStateMap(
      rhsArgumentStateMapId,
      new ArgumentStateBuffer.Factory(stateFactory, id), memoryTracker)
    this
  }

  override def nextTasks(accAndMorsel: Buffers.AccumulatorAndPayload[Morsel, HashTable, Morsel]): IndexedSeq[ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, HashTable]] =
    singletonIndexedSeq(new OTask(accAndMorsel.acc, accAndMorsel.payload))

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: HashTable, rhsMorsel: Morsel)
    extends InputLoopTask(rhsMorsel)
    with ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, HashTable] {

    override def workIdentity: WorkIdentity = NodeHashJoinOperator.this.workIdentity

    override def toString: String = "NodeHashJoinTask"

    private var lhsRows: java.util.Iterator[Morsel] = _
    private val key = new Array[Long](rhsOffsets.length)

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      fillKeyArray(inputCursor, key, rhsOffsets, rhsIsReference)
      lhsRows = accumulator.lhsRows(Values.longArray(key))
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && lhsRows.hasNext) {
        outputRow.copyFrom(lhsRows.next().readCursor(onFirstRow = true))
        copyDataFromRow(rhsLongMappings, rhsRefMappings, rhsCachedPropertyMappings, outputRow, inputCursor)
        outputRow.next()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}

object NodeHashJoinOperator {

  class HashTableFactory(lhsOffsets: KeyOffsets, memoryTracker: MemoryTracker) extends ArgumentStateFactory[HashTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): HashTable =
      new StandardHashTable(argumentRowId, lhsOffsets, argumentRowIdsForReducers, memoryTracker)
    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): HashTable =
      new ConcurrentHashTable(argumentRowId, lhsOffsets, argumentRowIdsForReducers)
  }

  /**
   * MorselAccumulator which groups rows by a tuple of node ids.
   */
  abstract class HashTable extends MorselAccumulator[Morsel] {
    def lhsRows(nodeIds: Value): java.util.Iterator[Morsel]
    def keys: util.Set[Value]
  }

  class StandardHashTable(override val argumentRowId: Long,
                          lhsKeyOffsets: KeyOffsets,
                          override val argumentRowIdsForReducers: Array[Long],
                          memoryTracker: MemoryTracker,
                          acceptNulls: Boolean = false) extends HashTable {
    private val table: ProbeTable[Value, Morsel] = ProbeTable.createProbeTable[Value, Morsel]( memoryTracker )

    private val lhsOffsets: Array[Int] = lhsKeyOffsets.offsets
    private val lhsIsReference: Array[Boolean] = lhsKeyOffsets.isReference

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        val key = new Array[Long](lhsOffsets.length)
        fillKeyArray(cursor, key, lhsOffsets, lhsIsReference)
        val isNullKey = NullChecker.entityIsNull(key(0))
        if (acceptNulls || !isNullKey) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          val view = morsel.view(cursor.row, cursor.row + 1)
          val keyValue = if (isNullKey) Values.NO_VALUE else Values.longArray(key)
          table.put(keyValue, view) // NOTE: ProbeTable will also track estimated heap usage of the view until the table is closed
        }
      }
    }

    override def lhsRows(nodeIds: Value): util.Iterator[Morsel] = {
      table.get(nodeIds)
    }
    override def keys: util.Set[Value] = table.keySet()

    override def close(): Unit = {
      table.close()
      super.close()
    }
  }

  class ConcurrentHashTable(override val argumentRowId: Long,
                            lhsKeyOffsets: KeyOffsets,
                            override val argumentRowIdsForReducers: Array[Long],
                            acceptNulls: Boolean = false) extends HashTable {
    private val table = new ConcurrentHashMap[Value, ConcurrentLinkedQueue[Morsel]]()

    private val lhsOffsets: Array[Int] = lhsKeyOffsets.offsets
    private val lhsIsReference: Array[Boolean] = lhsKeyOffsets.isReference

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        val key = new Array[Long](lhsOffsets.length)
        fillKeyArray(cursor, key, lhsOffsets, lhsIsReference)
        val isNullKey = NullChecker.entityIsNull(key(0))
        if (acceptNulls || !isNullKey) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          val keyValue = if (isNullKey) Values.NO_VALUE else Values.longArray(key)
          val lhsRows = table.computeIfAbsent(keyValue, _ => new ConcurrentLinkedQueue[Morsel]())
          lhsRows.add(morsel.view(cursor.row, cursor.row + 1))
        }
      }
    }

    override def lhsRows(nodeIds: Value): util.Iterator[Morsel] = {
      val lhsRows = table.get(nodeIds)
      if (lhsRows == null)
        util.Collections.emptyIterator()
      else
        lhsRows.iterator()
    }

    override def keys: util.Set[Value] = table.keySet()
  }
}
