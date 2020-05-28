/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.collection.DefaultComparatorTopTable
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.TopOperator.TopTable
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

/**
 * Reducing operator which collects pre-sorted input morsels until it
 * has seen all, and then streams the top `limit` rows from these morsels in
 * sorted order. Like all reducing operators, [[TopOperator]]
 * collects and streams grouped by argument rows ids.
 */
case class TopOperator(workIdentity: WorkIdentity,
                       orderBy: Seq[ColumnOrder],
                       countExpression: Expression) {

  override def toString: String = "TopMerge"

  private val comparator: Comparator[ReadableRow] = MorselSorting.createComparator(orderBy)

  def mapper(argumentSlotOffset: Int, outputBufferId: BufferId): TopMapperOperator =
    new TopMapperOperator(argumentSlotOffset, outputBufferId)

  def reducer(argumentStateMapId: ArgumentStateMapId, operatorId: Id): TopReduceOperator =
    new TopReduceOperator(argumentStateMapId)(operatorId)

  /**
   * Pre-operator for pre-sorting and limiting morsels. The operator performs local sorting/limiting of the
   * data in a single morsel, before putting these locally processed morsels into the
   * [[ExecutionState]] buffer which writes the rows into a top table.
   */
  class TopMapperOperator(argumentSlotOffset: Int,
                          outputBufferId: BufferId) extends OutputOperator {

    override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[Morsel]]](outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[Morsel]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = TopOperator.this.workIdentity
      override def trackTime: Boolean = true

      override def prepareOutput(morsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreTopOutput = {
        val limit = CountingState.evaluateCountValue(state, resources, countExpression)
        val perArguments = if (limit <= 0) {
          IndexedSeq.empty
        } else {
          ArgumentStateMap.map(argumentSlotOffset, morsel, argumentMorsel => argumentMorsel)
        }
        new PreTopOutput(perArguments, sink)
      }
    }

    class PreTopOutput(preTopped: IndexedSeq[PerArgument[Morsel]],
                       sink: Sink[IndexedSeq[PerArgument[Morsel]]]) extends PreparedOutput {
      override def produce(resources: QueryResources): Unit = {
        sink.put(preTopped, resources)
      }
    }

  }

  /**
   * Operator which streams sorted and limited data, built by [[TopMapperOperator]] and [[TopTable]].
   */
  class TopReduceOperator(argumentStateMapId: ArgumentStateMapId)
                         (val id: Id = Id.INVALID_ID)
    extends Operator
    with AccumulatorsInputOperatorState[Morsel, TopTable] {

    override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

    override def accumulatorsPerTask(morselSize: Int): Int = 1

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): AccumulatorsInputOperatorState[Morsel, TopTable] = {
      val limit = CountingState.evaluateCountValue(state, resources, countExpression)
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new TopOperator.Factory(memoryTracker, comparator, limit))
      this
    }

    override def nextTasks(input: IndexedSeq[TopTable]): IndexedSeq[ContinuableOperatorTaskWithAccumulators[Morsel, TopTable]] = {
      singletonIndexedSeq(new OTask(input))
    }

    class OTask(override val accumulators: IndexedSeq[TopTable]) extends ContinuableOperatorTaskWithAccumulators[Morsel, TopTable] {

      override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

      override def toString: String = "TopMergeTask"

      AssertMacros.checkOnlyWhenAssertionsAreEnabled(accumulators.size == 1)
      private val accumulator = accumulators.head
      private var sortedInputPerArgument: util.Iterator[MorselRow] = _

      override def operate(outputMorsel: Morsel,
                           state: PipelinedQueryState,
                           resources: QueryResources): Unit = {

        if (sortedInputPerArgument == null) {
          sortedInputPerArgument = accumulator.topRows()
        }

        val outputCursor = outputMorsel.writeCursor()
        while (outputCursor.next() && canContinue) {
          val nextRow: MorselRow = sortedInputPerArgument.next()
          outputCursor.copyFrom(nextRow)
        }

        if (!sortedInputPerArgument.hasNext) {
          accumulator.close()
        }

        outputCursor.truncate()
      }

      override def canContinue: Boolean = sortedInputPerArgument.hasNext
    }

  }

}

object TopOperator {

  class Factory(memoryTracker: MemoryTracker, comparator: Comparator[ReadableRow], limit: Long) extends ArgumentStateFactory[TopTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): TopTable =
      if (limit <= 0) {
        ZeroTable(argumentRowId, argumentRowIdsForReducers)
      } else {
        new StandardTopTable(argumentRowId, argumentRowIdsForReducers, memoryTracker, comparator, limit)
      }

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): TopTable =
      if (limit <= 0) {
        ZeroTable(argumentRowId, argumentRowIdsForReducers)
      } else {
        new ConcurrentTopTable(argumentRowId, argumentRowIdsForReducers, comparator, limit)
      }
  }

  /**
   * MorselAccumulator which returns top `limit` rows
   */
  abstract class TopTable extends MorselAccumulator[Morsel] {
    private var sorted = false

    final def topRows(): java.util.Iterator[MorselRow] = {
      if (sorted) {
        throw new IllegalArgumentException("Method should not be called more than once, per top table instance")
      }
      val rows = getTopRows
      sorted = true
      rows
    }

    protected def getTopRows: java.util.Iterator[MorselRow]
  }

  case class ZeroTable(override val argumentRowId: Long,
                       override val argumentRowIdsForReducers: Array[Long]) extends TopTable {
    // expected to be called by reduce task
    override protected def getTopRows: util.Iterator[MorselRow] = util.Collections.emptyIterator()

    override def update(data: Morsel, resources: QueryResources): Unit =
      error()

    private def error() =
      throw new IllegalStateException("Top table method should never be called with LIMIT 0")
  }

  class StandardTopTable(override val argumentRowId: Long,
                         override val argumentRowIdsForReducers: Array[Long],
                         memoryTracker: MemoryTracker,
                         comparator: Comparator[ReadableRow],
                         limit: Long) extends TopTable {

    private val topTable = new DefaultComparatorTopTable(comparator, limit, memoryTracker)
    private var totalTopHeapUsage = 0L
    private var morselCount = 0L
    private var maxMorselHeapUsage = 0L

    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      var hasAddedRow = false
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        if (topTable.add(cursor.snapshot()) && !(memoryTracker eq EmptyMemoryTracker.INSTANCE) && !hasAddedRow) {
          // track memory max once per morsel, and only if rows actually go into top table
          hasAddedRow = true
          // assume worst case, that every row in top table is from a different morsel
          morselCount = Math.min(morselCount + 1, limit)
          maxMorselHeapUsage = Math.max(maxMorselHeapUsage, morsel.estimatedHeapUsage)
          memoryTracker.releaseHeap(totalTopHeapUsage)
          totalTopHeapUsage = maxMorselHeapUsage * morselCount
          memoryTracker.allocateHeap(totalTopHeapUsage)
        }
      }
    }

    override def close(): Unit = {
      memoryTracker.releaseHeap(totalTopHeapUsage)
      totalTopHeapUsage = 0
      topTable.close()
    }

    override protected def getTopRows: util.Iterator[MorselRow] = {
      topTable.sort()
      topTable.iterator().asInstanceOf[util.Iterator[MorselRow]]
    }
  }

  class ConcurrentTopTable(override val argumentRowId: Long,
                           override val argumentRowIdsForReducers: Array[Long],
                           comparator: Comparator[ReadableRow],
                           limit: Long) extends TopTable {

    private val topTableByThread = new ConcurrentHashMap[Long, DefaultComparatorTopTable[ReadableRow]]

    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      val threadId = Thread.currentThread().getId
      val topTable = topTableByThread.computeIfAbsent(threadId, _ => new DefaultComparatorTopTable(comparator, limit))
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        topTable.add(cursor.snapshot())
      }
    }

    override protected def getTopRows: util.Iterator[MorselRow] = {
      val topTables = topTableByThread.values().iterator()
      if (!topTables.hasNext) {
        util.Collections.emptyIterator()
      } else {
        val mergedTopTable = topTables.next()
        while (topTables.hasNext) {
          val topTable = topTables.next()
          topTable.unorderedIterator().forEachRemaining(mergedTopTable.add)
        }
        mergedTopTable.sort()
        mergedTopTable.iterator().asInstanceOf[util.Iterator[MorselRow]]
      }
    }
  }

}

