/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFactory
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator.HashTable
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator.HashTableFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.copyDataFromRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.storable.Values

class NodeRightOuterHashJoinOperator(val workIdentity: WorkIdentity,
                                     lhsArgumentStateMapId: ArgumentStateMapId,
                                     rhsArgumentStateMapId: ArgumentStateMapId,
                                     lhsKeyOffsets: KeyOffsets,
                                     rhsKeyOffsets: KeyOffsets,
                                     lhsSlots: SlotConfiguration,
                                     lhsSlotMappings: SlotMappings)
                          (val id: Id = Id.INVALID_ID) extends Operator with AccumulatorsAndMorselInputOperatorState[Morsel, HashTable] {

  private val lhsLongMappings: Array[(Int, Int)] = lhsSlotMappings.longMappings
  private val lhsRefMappings: Array[(Int, Int)] = lhsSlotMappings.refMappings
  private val lhsCachedPropertyMappings: Array[(Int, Int)] = lhsSlotMappings.cachedPropertyMappings
  private val rhsOffsets: Array[Int] = rhsKeyOffsets.offsets
  private val rhsIsReference: Array[Boolean] = rhsKeyOffsets.isReference

  private val emptyLhsRowMorsel = {
    val morsel = MorselFactory.allocate(lhsSlots, 1)
    val row = morsel.writeCursor(onFirstRow = true)
    lhsLongMappings.foreach { case (from, _) =>
      row.setLongAt(from, StatementConstants.NO_SUCH_ENTITY)
    }
    lhsRefMappings.foreach { case (from, _) =>
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
      new HashTableFactory(lhsKeyOffsets, memoryTracker))
    argumentStateCreator.createArgumentStateMap(
      rhsArgumentStateMapId,
      new ArgumentStateBuffer.Factory(stateFactory, id))
    this
  }

  override def nextTasks(accAndMorsel: Buffers.AccumulatorAndMorsel[Morsel, HashTable]): IndexedSeq[ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, HashTable]] =
    singletonIndexedSeq(new OTask(accAndMorsel.acc, accAndMorsel.morsel))

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: HashTable, rhsMorsel: Morsel)
    extends InputLoopTask(rhsMorsel)
    with ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, HashTable] {

    override def workIdentity: WorkIdentity = NodeRightOuterHashJoinOperator.this.workIdentity

    override def toString: String = "NodeRightOuterHashJoinTask"

    private var lhsRows: java.util.Iterator[Morsel] = _
    private val key = new Array[Long](rhsOffsets.length)

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      fillKeyArray(inputCursor, key, rhsOffsets, rhsIsReference)
      lhsRows = accumulator.lhsRows(Values.longArray(key))
      if (!lhsRows.hasNext) {
        // No matches on LHS, so return empty row
        lhsRows = Iterators.iterator[Morsel](emptyLhsRowMorsel)
      }
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      while (outputRow.onValidRow && lhsRows.hasNext) {
        val lhsRow = lhsRows.next().readCursor(onFirstRow = true)
        copyDataFromRow(lhsLongMappings, lhsRefMappings, lhsCachedPropertyMappings, outputRow, lhsRow)
        outputRow.copyFrom(inputCursor)
        outputRow.next()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}


