/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

import org.eclipse.collections.impl.factory.Multimaps
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
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
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values

class NodeHashJoinOperator(val workIdentity: WorkIdentity,
                           lhsArgumentStateMapId: ArgumentStateMapId,
                           rhsArgumentStateMapId: ArgumentStateMapId,
                           lhsOffsets: Array[Int],
                           rhsOffsets: Array[Int],
                           slots: SlotConfiguration,
                           longsToCopy: Array[(Int, Int)],
                           refsToCopy: Array[(Int, Int)],
                           cachedPropertiesToCopy: Array[(Int, Int)])
                          (val id: Id = Id.INVALID_ID) extends Operator with OperatorState {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(
      lhsArgumentStateMapId,
      new HashTableFactory(lhsOffsets, stateFactory.memoryTracker, id))
    argumentStateCreator.createArgumentStateMap(
      rhsArgumentStateMapId,
      new ArgumentStateBuffer.Factory(stateFactory, id))
    this
  }

  override def nextTasks(state: PipelinedQueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, HashTable]] = {
    val accAndMorsel = operatorInput.takeAccumulatorAndMorsel()
    if (accAndMorsel != null) {
      Array(new OTask(accAndMorsel.acc, accAndMorsel.morsel))
    } else {
      null
    }

  }

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: HashTable, rhsMorsel: Morsel)
    extends InputLoopTask(rhsMorsel)
    with ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, HashTable] {

    override def workIdentity: WorkIdentity = NodeHashJoinOperator.this.workIdentity

    override def toString: String = "NodeHashJoinTask"

    private var lhsRows: java.util.Iterator[Morsel] = _
    private val key = new Array[Long](rhsOffsets.length)

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      fillKeyArray(inputCursor, key, rhsOffsets)
      lhsRows = accumulator.lhsRows(Values.longArray(key))
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && lhsRows.hasNext) {
        outputRow.copyFrom(lhsRows.next().readCursor(onFirstRow = true))
        NodeHashJoinSlottedPipe.copyDataFromRhs(longsToCopy, refsToCopy, cachedPropertiesToCopy, outputRow, inputCursor)
        outputRow.next()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}

object NodeHashJoinOperator {

  class HashTableFactory(lhsOffsets: Array[Int], memoryTracker: QueryMemoryTracker, operatorId: Id) extends ArgumentStateFactory[HashTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): HashTable =
      new StandardHashTable(argumentRowId, lhsOffsets, argumentRowIdsForReducers, memoryTracker, operatorId)
    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): HashTable =
      new ConcurrentHashTable(argumentRowId, lhsOffsets, argumentRowIdsForReducers)
  }

  /**
   * MorselAccumulator which groups rows by a tuple of node ids.
   */
  abstract class HashTable extends MorselAccumulator[Morsel] {
    def lhsRows(nodeIds: LongArray): java.util.Iterator[Morsel]

  }

  class StandardHashTable(override val argumentRowId: Long,
                          lhsOffsets: Array[Int],
                          override val argumentRowIdsForReducers: Array[Long],
                          memoryTracker: QueryMemoryTracker,
                          operatorId: Id) extends HashTable {
    private val table = Multimaps.mutable.list.empty[LongArray, Morsel]()

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: Morsel): Unit = {
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        val key = new Array[Long](lhsOffsets.length)
        fillKeyArray(cursor, key, lhsOffsets)
        if (!NullChecker.entityIsNull(key(0))) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          val view = morsel.view(cursor.row, cursor.row + 1)
          table.put(Values.longArray(key), view)
          // Note: this allocation is currently never de-allocated
          memoryTracker.allocated(view, operatorId.x)
        }
      }
    }

    override def lhsRows(nodeIds: LongArray): util.Iterator[Morsel] =
      table.get(nodeIds).iterator()
  }

  class ConcurrentHashTable(override val argumentRowId: Long,
                            lhsOffsets: Array[Int],
                            override val argumentRowIdsForReducers: Array[Long]) extends HashTable {
    private val table = new ConcurrentHashMap[LongArray, ConcurrentLinkedQueue[Morsel]]()

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: Morsel): Unit = {
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        val key = new Array[Long](lhsOffsets.length)
        fillKeyArray(cursor, key, lhsOffsets)
        if (!NullChecker.entityIsNull(key(0))) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          val lhsRows = table.computeIfAbsent(Values.longArray(key), _ => new ConcurrentLinkedQueue[Morsel]())
          lhsRows.add(morsel.view(cursor.row, cursor.row + 1))
        }
      }
    }

    override def lhsRows(nodeIds: LongArray): util.Iterator[Morsel] = {
      val lhsRows = table.get(nodeIds)
      if (lhsRows == null)
        util.Collections.emptyIterator()
      else
        lhsRows.iterator()
    }
  }
}
