/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.eclipse.collections.impl.factory.Multimaps
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.zombie.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.zombie.operators.NodeHashJoinOperator.HashTable
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.LHSAccumulatingRHSStreamingBuffer
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
      argumentRowId => new HashTable(argumentRowId, lhsOffsets))
    argumentStateCreator.createArgumentStateMap(
      rhsArgumentStateMapId,
      argumentRowId => new LHSAccumulatingRHSStreamingBuffer.RHSBuffer(argumentRowId, argumentStateCreator.newBuffer[MorselExecutionContext]()))
    this
  }

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         operatorInput: OperatorInput,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulator[HashTable]] = {
    val tuple = operatorInput.takeAccumulatorAndMorsel()
    if (tuple != null) {
      Array(new OTask(tuple._1, tuple._2))
    } else {
      null
    }

  }

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: HashTable, rhsRow: MorselExecutionContext)
    extends InputLoopTask
      with ContinuableOperatorTaskWithMorselAndAccumulator[HashTable] {

    override val inputMorsel: MorselExecutionContext = rhsRow

    override def toString: String = "NodeHashJoinTask"

    private var lhsRows: java.util.Iterator[MorselExecutionContext] = _
    private val key = new Array[Long](rhsOffsets.length)

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): Boolean = {
      NodeHashJoinOperator.fillKeyArray(rhsRow, key, rhsOffsets)
      lhsRows = accumulator.table.get(Values.longArray(key)).iterator()
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && lhsRows.hasNext) {
        outputRow.copyFrom(lhsRows.next())
        copyDataFromRhs(outputRow, rhsRow)
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = ()
  }

  private def copyDataFromRhs(newRow: MorselExecutionContext, rhs: MorselExecutionContext): Unit = {
    var i = 0
    while (i < longsToCopy.length) {
      val fromTo = longsToCopy(i)
      newRow.setLongAt(fromTo._2, rhs.getLongAt(fromTo._1))
      i += 1
    }
    i = 0
    while (i < refsToCopy.length) {
      val fromTo = refsToCopy(i)
      newRow.setRefAt(fromTo._2, rhs.getRefAt(fromTo._1))
      i += 1
    }
    i = 0
    while (i < cachedPropertiesToCopy.length) {
      val fromTo = cachedPropertiesToCopy(i)
      newRow.setCachedPropertyAt(fromTo._2, rhs.getCachedPropertyAt(fromTo._1))
      i += 1
    }
  }
}

object NodeHashJoinOperator {

  private def fillKeyArray(currentMorsel: MorselExecutionContext, key: Array[Long], offsets: Array[Int]): Unit = {
    // We use a while loop like this to be able to break out early
    var i = 0
    while (i < offsets.length) {
      val thisId = currentMorsel.getLongAt(offsets(i))
      key(i) = thisId
      if (NullChecker.entityIsNull(thisId)) {
        key(0) = -1 // We flag the null in this cryptic way to avoid creating objects
        return
      }
      i += 1
    }
  }

  class HashTable(override val argumentRowId: Long,
                  lhsOffsets: Array[Int]) extends MorselAccumulator {
    private[NodeHashJoinOperator] val table = Multimaps.mutable.list.empty[LongArray, MorselExecutionContext]()

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
  }

}
