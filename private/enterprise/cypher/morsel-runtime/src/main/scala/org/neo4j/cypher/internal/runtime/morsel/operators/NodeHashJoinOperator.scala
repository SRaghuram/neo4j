/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.eclipse.collections.impl.factory.Multimaps
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.runtime.morsel.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.NodeHashJoinOperator.{HashTable, HashTableFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.ArgumentStateBuffer
import org.neo4j.values.storable.{LongArray, Values}

class NodeHashJoinOperator(val workIdentity: WorkIdentity,
                           lhsArgumentStateMapId: ArgumentStateMapId,
                           rhsArgumentStateMapId: ArgumentStateMapId,
                           lhsOffsets: Array[Int],
                           rhsOffsets: Array[Int],
                           slots: SlotConfiguration,
                           longsToCopy: Array[(Int, Int)],
                           refsToCopy: Array[(Int, Int)],
                           cachedPropertiesToCopy: Array[(Int, Int)]) extends Operator with OperatorState {

  override def createState(argumentStateCreator: ArgumentStateMapCreator): OperatorState = {
    argumentStateCreator.createArgumentStateMap(
      lhsArgumentStateMapId,
      new HashTableFactory(lhsOffsets))
    argumentStateCreator.createArgumentStateMap(
      rhsArgumentStateMapId,
      ArgumentStateBuffer.Factory)
    this
  }

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         operatorInput: OperatorInput,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulator[MorselExecutionContext, HashTable]] = {
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

    override val inputMorsel: MorselExecutionContext = rhsRow

    override def toString: String = "NodeHashJoinTask"

    private var lhsRows: java.util.Iterator[MorselExecutionContext] = _
    private val key = new Array[Long](rhsOffsets.length)

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): Boolean = {
      fillKeyArray(rhsRow, key, rhsOffsets)
      lhsRows = accumulator.lhsRows(Values.longArray(key))
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

    override protected def closeInnerLoop(resources: QueryResources): Unit = ()
  }

}

object NodeHashJoinOperator {

  class HashTableFactory(lhsOffsets: Array[Int]) extends ArgumentStateFactory[HashTable] {
    override def newStandardArgumentState(argumentRowId: Long): HashTable = new StandardHashTable(argumentRowId, lhsOffsets)
    override def newConcurrentArgumentState(argumentRowId: Long): HashTable = new ConcurrentHashTable(argumentRowId, lhsOffsets)
  }

  /**
    * MorselAccumulator which groups rows by a tuple of node ids.
    */
  abstract class HashTable extends MorselAccumulator[MorselExecutionContext] {
    def lhsRows(nodeIds: LongArray): java.util.Iterator[MorselExecutionContext]

  }

  class StandardHashTable(override val argumentRowId: Long,
                          lhsOffsets: Array[Int]) extends HashTable {
    private val table = Multimaps.mutable.list.empty[LongArray, MorselExecutionContext]()

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: MorselExecutionContext): Unit = {
      while (morsel.isValidRow) {
        val key = new Array[Long](lhsOffsets.length)
        fillKeyArray(morsel, key, lhsOffsets)
        if (!NullChecker.entityIsNull(key(0))) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          table.put(Values.longArray(key), morsel.shallowCopy())
        }
        morsel.moveToNextRow()
      }
    }

    override def lhsRows(nodeIds: LongArray): util.Iterator[MorselExecutionContext] =
      table.get(nodeIds).iterator()
  }

  class ConcurrentHashTable(override val argumentRowId: Long,
                          lhsOffsets: Array[Int]) extends HashTable {
    private val table = new ConcurrentHashMap[LongArray, ConcurrentLinkedQueue[MorselExecutionContext]]()

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: MorselExecutionContext): Unit = {
      while (morsel.isValidRow) {
        val key = new Array[Long](lhsOffsets.length)
        fillKeyArray(morsel, key, lhsOffsets)
        if (!NullChecker.entityIsNull(key(0))) {
          // TODO optimize this to something like this
          //        val lastMorsel = morselsForKey.last
          //        if (!lastMorsel.hasNextRow) {
          //          // create new morsel and add to morselsForKey
          //        }
          //        lastMorsel.moveToNextRow()
          //        lastMorsel.copyFrom(morsel)
          val lhsRows = table.computeIfAbsent(Values.longArray(key), _ => new ConcurrentLinkedQueue[MorselExecutionContext]())
          lhsRows.add(morsel.shallowCopy())
        }
        morsel.moveToNextRow()
      }
    }

    override def lhsRows(nodeIds: LongArray): util.Iterator[MorselExecutionContext] = {
      val lhsRows = table.get(nodeIds)
      if (lhsRows == null)
        util.Collections.emptyIterator()
      else
        lhsRows.iterator()
    }
  }
}
