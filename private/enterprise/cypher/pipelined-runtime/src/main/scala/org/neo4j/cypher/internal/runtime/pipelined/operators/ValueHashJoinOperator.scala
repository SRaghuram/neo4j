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
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ValueHashJoinOperator.HashTable
import org.neo4j.cypher.internal.runtime.pipelined.operators.ValueHashJoinOperator.HashTableFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values


class ValueHashJoinOperator(val workIdentity: WorkIdentity,
                            lhsArgumentStateMapId: ArgumentStateMapId,
                            rhsArgumentStateMapId: ArgumentStateMapId,
                            slots: SlotConfiguration,
                            lhsExpression: Expression,
                            rhsExpression: Expression,
                            longOffset: Int,
                            refsOffset: Int,
                            argumentSize: SlotConfiguration.Size)
                           (val id: Id = Id.INVALID_ID) extends Operator with OperatorState {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(
      lhsArgumentStateMapId, new HashTableFactory(memoryTracker, lhsExpression, state))
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

  class OTask(override val accumulator: HashTable, rhsMorsel: Morsel)
    extends InputLoopTask(rhsMorsel)
    with ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, HashTable] {

    override def workIdentity: WorkIdentity = ValueHashJoinOperator.this.workIdentity

    override def toString: String = "ValueHashJoinTask"

    private var lhsRows: java.util.Iterator[Morsel] = _

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      val queryState = state.queryStateForExpressionEvaluation(resources)
      val key = rhsExpression.apply(inputCursor, queryState)
      lhsRows = accumulator.lhsRows(key)
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      while (outputRow.onValidRow && lhsRows.hasNext) {
        val morsel = lhsRows.next()
        outputRow.copyFrom(morsel.readCursor(onFirstRow = true))
        inputCursor.copyTo(outputRow, sourceLongOffset = argumentSize.nLongs, sourceRefOffset = argumentSize.nReferences, targetLongOffset = longOffset, targetRefOffset = refsOffset)

        outputRow.next()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}

object ValueHashJoinOperator {

  class HashTableFactory(memoryTracker: MemoryTracker, expression: Expression, state: PipelinedQueryState) extends ArgumentStateFactory[HashTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): HashTable =
      new StandardHashTable(argumentRowId, argumentRowIdsForReducers, expression, state, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): HashTable =
      new ConcurrentHashTable(argumentRowId, argumentRowIdsForReducers, expression, state)
  }

  /**
   * MorselAccumulator which groups rows by a tuple of node ids.
   */
  abstract class HashTable extends MorselAccumulator[Morsel] {
    def lhsRows(key: AnyValue): java.util.Iterator[Morsel]

  }

  class StandardHashTable(override val argumentRowId: Long,
                          override val argumentRowIdsForReducers: Array[Long],
                          expression: Expression,
                          state: PipelinedQueryState,
                          memoryTracker: MemoryTracker) extends HashTable {
    private val table = ProbeTable.createProbeTable[AnyValue, Morsel]( memoryTracker )

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      val cursor = morsel.readCursor()
      val queryState = state.queryStateForExpressionEvaluation(resources)
      while (cursor.next()) {
        val key = expression.apply(cursor, queryState)
        if (key != Values.NO_VALUE) {
          val view = morsel.view(cursor.row, cursor.row + 1)
          table.put(key, view) // NOTE: ProbeTable will also track estimated heap usage of the view until the table is closed
        }
      }
    }

    override def lhsRows(key: AnyValue): util.Iterator[Morsel] = {
      table.get(key)
    }

    override def close(): Unit = {
      table.close()
      super.close()
    }
  }

  class ConcurrentHashTable(override val argumentRowId: Long,
                            override val argumentRowIdsForReducers: Array[Long],
                            expression: Expression,
                            state: PipelinedQueryState) extends HashTable {
    private val table = new ConcurrentHashMap[AnyValue, ConcurrentLinkedQueue[Morsel]]()

    // This is update from LHS, i.e. we need to put stuff into a hash table
    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      val queryState = state.queryStateForExpressionEvaluation(resources)
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        val key =  expression.apply(cursor, queryState)
        if (key != Values.NO_VALUE) {
          val lhsRows = table.computeIfAbsent(key, _ => new ConcurrentLinkedQueue[Morsel]())
          lhsRows.add(morsel.view(cursor.row, cursor.row + 1))
        }
      }
    }

    override def lhsRows(key: AnyValue): util.Iterator[Morsel] = {
      val lhsRows = table.get(key)
      if (lhsRows == null)
        util.Collections.emptyIterator()
      else
        lhsRows.iterator()
    }
  }
}


