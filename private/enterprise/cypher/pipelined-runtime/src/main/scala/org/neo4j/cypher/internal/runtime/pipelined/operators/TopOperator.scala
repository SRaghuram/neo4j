/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.DefaultComparatorTopTable
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
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
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.util.attribution.Id

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

  private val comparator: Comparator[MorselRow] = MorselSorting.createComparator(orderBy)

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

    override def createState(executionState: ExecutionState): OutputOperatorState =
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
      override def produce(): Unit = {
        sink.put(preTopped)
      }
    }

  }

  /**
   * Operator which streams sorted and limited data, built by [[TopMapperOperator]] and [[TopTable]].
   */
  class TopReduceOperator(argumentStateMapId: ArgumentStateMapId)
                         (val id: Id = Id.INVALID_ID)
    extends Operator
    with ReduceOperatorState[Morsel, TopTable] {

    override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

    override def accumulatorsPerTask(morselSize: Int): Int = 1

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): ReduceOperatorState[Morsel, TopTable] = {
      // NOTE: If the _input size_ is larger than Int.MaxValue this will still fail, since an array cannot hold that many elements
      val limit = Math.min(CountingState.evaluateCountValue(state, resources, countExpression), Int.MaxValue).toInt
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new TopOperator.Factory(stateFactory.memoryTracker, comparator, limit, id), ordered = false)
      this
    }

    override def nextTasks(state: PipelinedQueryState, input: IndexedSeq[TopTable], resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[Morsel, TopTable]] = {
      Array(new OTask(input))
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
          accumulator.deallocateMemory()
        }

        outputCursor.truncate()
      }

      override def canContinue: Boolean = sortedInputPerArgument.hasNext
    }

  }

}

object TopOperator {

  class Factory(memoryTracker: QueryMemoryTracker, comparator: Comparator[MorselRow], limit: Int, operatorId: Id) extends ArgumentStateFactory[TopTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): TopTable =
      if (limit <= 0) {
        ZeroTable(argumentRowId, argumentRowIdsForReducers)
      } else {
        new StandardTopTable(argumentRowId, argumentRowIdsForReducers, memoryTracker, comparator, limit, operatorId)
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

    def deallocateMemory(): Unit

    protected def getTopRows: java.util.Iterator[MorselRow]
  }

  case class ZeroTable(override val argumentRowId: Long,
                       override val argumentRowIdsForReducers: Array[Long]) extends TopTable {
    // expected to be called by reduce task
    override protected def getTopRows: util.Iterator[MorselRow] = util.Collections.emptyIterator()

    override def update(data: Morsel): Unit =
      error()

    // expected to be called by reduce task
    override def deallocateMemory(): Unit = {}

    private def error() =
      throw new IllegalStateException("Top table method should never be called with LIMIT 0")
  }

  class StandardTopTable(override val argumentRowId: Long,
                         override val argumentRowIdsForReducers: Array[Long],
                         memoryTracker: QueryMemoryTracker,
                         comparator: Comparator[MorselRow],
                         limit: Int,
                         operatorId: Id) extends TopTable {

    private val topTable = new DefaultComparatorTopTable(comparator, limit)
    private var totalTopHeapUsage = 0L
    private var morselCount = 0
    private var maxMorselHeapUsage = 0L

    override def update(morsel: Morsel): Unit = {
      var hasAddedRow = false
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        if (topTable.add(cursor.snapshot()) && memoryTracker.isEnabled && !hasAddedRow) {
          // track memory max once per morsel, and only if rows actually go into top table
          hasAddedRow = true
          // assume worst case, that every row in top table is from a different morsel
          morselCount = Math.min(morselCount + 1, limit)
          maxMorselHeapUsage = Math.max(maxMorselHeapUsage, morsel.estimatedHeapUsage)
          memoryTracker.deallocated(totalTopHeapUsage, operatorId.x)
          totalTopHeapUsage = maxMorselHeapUsage * morselCount
          memoryTracker.allocated(totalTopHeapUsage, operatorId.x)
        }
      }
    }

    override def deallocateMemory(): Unit = {
      memoryTracker.deallocated(totalTopHeapUsage, operatorId.x)
    }

    override protected def getTopRows: util.Iterator[MorselRow] = {
      topTable.sort()
      topTable.iterator()
    }
  }

  class ConcurrentTopTable(override val argumentRowId: Long,
                           override val argumentRowIdsForReducers: Array[Long],
                           comparator: Comparator[MorselRow],
                           limit: Int) extends TopTable {

    private val topTableByThread = new ConcurrentHashMap[Long, DefaultComparatorTopTable[MorselRow]]

    override def update(morsel: Morsel): Unit = {
      val threadId = Thread.currentThread().getId
      val topTable = topTableByThread.computeIfAbsent(threadId, _ => new DefaultComparatorTopTable(comparator, limit))
      val cursor = morsel.readCursor()
      while (cursor.next()) {
        topTable.add(cursor.snapshot())
      }
    }

    override def deallocateMemory(): Unit = {}

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
        mergedTopTable.iterator()
      }
    }
  }

}

