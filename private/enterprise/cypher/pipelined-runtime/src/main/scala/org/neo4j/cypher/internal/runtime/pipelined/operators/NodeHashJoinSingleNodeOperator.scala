/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

import org.eclipse.collections.impl.factory.primitive.LongObjectMaps
import org.eclipse.collections.impl.list.mutable.FastList
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinSingleNodeOperator.HashTable
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinSingleNodeOperator.HashTableFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe

class NodeHashJoinSingleNodeOperator(val workIdentity: WorkIdentity,
                                     lhsArgumentStateMapId: ArgumentStateMapId,
                                     rhsArgumentStateMapId: ArgumentStateMapId,
                                     lhsOffset: Int,
                                     rhsOffset: Int,
                                     slots: SlotConfiguration,
                                     longsToCopy: Array[(Int, Int)],
                                     refsToCopy: Array[(Int, Int)],
                                     cachedPropertiesToCopy: Array[(Int, Int)]) extends Operator with OperatorState {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(
      lhsArgumentStateMapId,
      new HashTableFactory(lhsOffset, stateFactory.memoryTracker))
    argumentStateCreator.createArgumentStateMap(
      rhsArgumentStateMapId,
      new ArgumentStateBuffer.Factory(stateFactory))
    this
  }

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithAccumulator[MorselExecutionContext, HashTable]] = {
    val accAndMorsel = operatorInput.takeAccumulatorAndMorsel()
    if (accAndMorsel != null) {
      Array(new OTask(accAndMorsel.acc, accAndMorsel.morsel))
    } else {
      null
    }

  }

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: HashTable, rhsRow: MorselExecutionContext)
    extends InputLoopTask
    with ContinuableOperatorTaskWithMorselAndAccumulator[MorselExecutionContext, HashTable] {

    override def workIdentity: WorkIdentity = NodeHashJoinSingleNodeOperator.this.workIdentity

    override val inputMorsel: MorselExecutionContext = rhsRow

    override def toString: String = "NodeHashJoinSingleNodeTask"

    private var lhsRows: java.util.Iterator[MorselExecutionContext] = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val key = rhsRow.getLongAt(rhsOffset)
      lhsRows = accumulator.lhsRows(key)
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && lhsRows.hasNext) {
        outputRow.copyFrom(lhsRows.next())
        NodeHashJoinSlottedPipe.copyDataFromRhs(longsToCopy, refsToCopy, cachedPropertiesToCopy, outputRow, rhsRow)
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}

object NodeHashJoinSingleNodeOperator {

  class HashTableFactory(lhsOffset: Int, memoryTracker: QueryMemoryTracker) extends ArgumentStateFactory[HashTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): HashTable =
      new StandardHashTable(argumentRowId, lhsOffset, argumentRowIdsForReducers, memoryTracker)
    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): HashTable =
      new ConcurrentHashTable(argumentRowId, lhsOffset, argumentRowIdsForReducers)
  }

  /**
   * MorselAccumulator which groups rows by a tuple of node ids.
   */
  abstract class HashTable extends MorselAccumulator[MorselExecutionContext] {
    def lhsRows(nodeId: Long): java.util.Iterator[MorselExecutionContext]

  }

  class StandardHashTable(override val argumentRowId: Long,
                          lhsOffset: Int,
                          override val argumentRowIdsForReducers: Array[Long],
                          memoryTracker: QueryMemoryTracker) extends HashTable {
    private val table = LongObjectMaps.mutable.empty[FastList[MorselExecutionContext]]()

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: MorselExecutionContext): Unit = {
      while (morsel.isValidRow) {
        val key = morsel.getLongAt(lhsOffset)
        if (!NullChecker.entityIsNull(key)) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          val view = morsel.view(morsel.getCurrentRow, morsel.getCurrentRow + 1)
          val list = table.getIfAbsentPut(key, new FastList[MorselExecutionContext](1))
          list.add(view)
          // Note: this allocation is currently never de-allocated
          memoryTracker.allocated(view)
        }
        morsel.moveToNextRow()
      }
    }

    override def lhsRows(nodeId: Long): util.Iterator[MorselExecutionContext] = {
      val lhsRows = table.get(nodeId)
      if (lhsRows == null)
        util.Collections.emptyIterator()
      else
        lhsRows.iterator()
    }
  }

  class ConcurrentHashTable(override val argumentRowId: Long,
                            lhsOffset: Int,
                            override val argumentRowIdsForReducers: Array[Long]) extends HashTable {
    private val table = new ConcurrentHashMap[Long, ConcurrentLinkedQueue[MorselExecutionContext]]()

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: MorselExecutionContext): Unit = {
      while (morsel.isValidRow) {
        val key = morsel.getLongAt(lhsOffset)
        if (!NullChecker.entityIsNull(key)) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          val lhsRows = table.computeIfAbsent(key, _ => new ConcurrentLinkedQueue[MorselExecutionContext]())
          lhsRows.add(morsel.shallowCopy())
        }
        morsel.moveToNextRow()
      }
    }

    override def lhsRows(nodeId: Long): util.Iterator[MorselExecutionContext] = {
      val lhsRows = table.get(nodeId)
      if (lhsRows == null)
        util.Collections.emptyIterator()
      else
        lhsRows.iterator()
    }
  }
}
