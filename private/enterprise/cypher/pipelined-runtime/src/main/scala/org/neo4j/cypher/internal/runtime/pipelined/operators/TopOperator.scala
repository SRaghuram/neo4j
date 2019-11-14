/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.DefaultComparatorTopTable
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.execution.{PipelinedExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.operators.TopOperator.TopTable
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator, PerArgument}
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.pipelined.state.{ArgumentStateMap, StateFactory}
import org.neo4j.cypher.internal.runtime.pipelined.{ArgumentStateMapCreator, ExecutionState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryMemoryTracker}

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

  private val comparator: Comparator[PipelinedExecutionContext] = MorselSorting.createComparator(orderBy)

  def mapper(argumentSlotOffset: Int, outputBufferId: BufferId): TopMapperOperator =
    new TopMapperOperator(argumentSlotOffset, outputBufferId)

  def reducer(argumentStateMapId: ArgumentStateMapId): TopReduceOperator =
    new TopReduceOperator(argumentStateMapId)

  /**
   * Pre-operator for pre-sorting and limiting morsels. The operator performs local sorting/limiting of the
   * data in a single morsel, before putting these locally processed morsels into the
   * [[ExecutionState]] buffer which writes the rows into a top table.
   */
  class TopMapperOperator(argumentSlotOffset: Int,
                          outputBufferId: BufferId) extends OutputOperator {

    override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState,
                             pipelineId: PipelineId): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[PipelinedExecutionContext]]](pipelineId, outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[PipelinedExecutionContext]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

      override def prepareOutput(morsel: PipelinedExecutionContext,
                                 queryContext: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreTopOutput = {
        val limit = LimitOperator.evaluateCountValue(queryContext, state, resources, countExpression)
        val perArguments = if (limit <= 0) {
          IndexedSeq.empty
        } else {
          ArgumentStateMap.map(argumentSlotOffset, morsel, argumentMorsel => argumentMorsel)
        }
        new PreTopOutput(perArguments, sink)
      }
    }

    class PreTopOutput(preTopped: IndexedSeq[PerArgument[PipelinedExecutionContext]],
                       sink: Sink[IndexedSeq[PerArgument[PipelinedExecutionContext]]]) extends PreparedOutput {
      override def produce(): Unit = {
        sink.put(preTopped)
      }
    }

  }

  /**
   * Operator which streams sorted and limited data, built by [[TopMapperOperator]] and [[TopTable]].
   */
  class TopReduceOperator(argumentStateMapId: ArgumentStateMapId)
    extends Operator
    with ReduceOperatorState[PipelinedExecutionContext, TopTable] {

    override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             queryContext: QueryContext,
                             state: QueryState,
                             resources: QueryResources): ReduceOperatorState[PipelinedExecutionContext, TopTable] = {
      // NOTE: If the _input size_ is larger than Int.MaxValue this will still fail, since an array cannot hold that many elements
      val limit = Math.min(LimitOperator.evaluateCountValue(queryContext, state, resources, countExpression), Int.MaxValue).toInt
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new TopOperator.Factory(stateFactory.memoryTracker, comparator, limit))
      this
    }

    override def nextTasks(queryContext: QueryContext,
                           state: QueryState,
                           input: TopTable,
                           resources: QueryResources
                          ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[PipelinedExecutionContext, TopTable]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulator: TopTable) extends ContinuableOperatorTaskWithAccumulator[PipelinedExecutionContext, TopTable] {

      override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

      override def toString: String = "TopMergeTask"

      var sortedInputPerArgument: util.Iterator[PipelinedExecutionContext] = _

      override def operate(outputRow: PipelinedExecutionContext,
                           context: QueryContext,
                           state: QueryState,
                           resources: QueryResources): Unit = {

        if (sortedInputPerArgument == null) {
          sortedInputPerArgument = accumulator.topRows()
        }

        while (outputRow.isValidRow && canContinue) {
          val nextRow: PipelinedExecutionContext = sortedInputPerArgument.next()
          outputRow.copyFrom(nextRow)
          nextRow.moveToNextRow()
          outputRow.moveToNextRow()
        }

        if (!sortedInputPerArgument.hasNext) {
          accumulator.deallocateMemory()
        }

        outputRow.finishedWriting()
      }

      override def canContinue: Boolean = sortedInputPerArgument.hasNext
    }

  }

}

object TopOperator {

  class Factory(memoryTracker: QueryMemoryTracker, comparator: Comparator[PipelinedExecutionContext], limit: Int) extends ArgumentStateFactory[TopTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: PipelinedExecutionContext, argumentRowIdsForReducers: Array[Long]): TopTable =
      if (limit <= 0) {
        ZeroTable(argumentRowId, argumentRowIdsForReducers)
      } else {
        new StandardTopTable(argumentRowId, argumentRowIdsForReducers, memoryTracker, comparator, limit)
      }

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: PipelinedExecutionContext, argumentRowIdsForReducers: Array[Long]): TopTable =
      if (limit <= 0) {
        ZeroTable(argumentRowId, argumentRowIdsForReducers)
      } else {
        new ConcurrentTopTable(argumentRowId, argumentRowIdsForReducers, comparator, limit)
      }
  }

  /**
   * MorselAccumulator which returns top `limit` rows
   */
  abstract class TopTable extends MorselAccumulator[PipelinedExecutionContext] {
    private var sorted = false

    final def topRows(): java.util.Iterator[PipelinedExecutionContext] = {
      if (sorted) {
        throw new IllegalArgumentException("Method should not be called more than once, per top table instance")
      }
      val rows = getTopRows
      sorted = true
      rows
    }

    def deallocateMemory(): Unit

    protected def getTopRows: java.util.Iterator[PipelinedExecutionContext]
  }

  case class ZeroTable(override val argumentRowId: Long,
                       override val argumentRowIdsForReducers: Array[Long]) extends TopTable {
    // expected to be called by reduce task
    override protected def getTopRows: util.Iterator[PipelinedExecutionContext] = util.Collections.emptyIterator()

    override def update(data: PipelinedExecutionContext): Unit =
      error()

    // expected to be called by reduce task
    override def deallocateMemory(): Unit = {}

    private def error() =
      throw new IllegalStateException("Top table method should never be called with LIMIT 0")
  }

  class StandardTopTable(override val argumentRowId: Long,
                         override val argumentRowIdsForReducers: Array[Long],
                         memoryTracker: QueryMemoryTracker,
                         comparator: Comparator[PipelinedExecutionContext],
                         limit: Int) extends TopTable {

    private val topTable = new DefaultComparatorTopTable(comparator, limit)
    private var totalTopHeapUsage = 0L
    private var morselCount = 0
    private var maxMorselHeapUsage = 0L

    override def update(morsel: PipelinedExecutionContext): Unit = {
      var hasAddedRow = false
      while (morsel.isValidRow) {
        if (topTable.add(morsel.shallowCopy()) && memoryTracker.isEnabled && !hasAddedRow) {
          // track memory max once per morsel, and only if rows actually go into top table
          hasAddedRow = true
          // assume worst case, that every row in top table is from a different morsel
          morselCount = Math.min(morselCount + 1, limit)
          maxMorselHeapUsage = Math.max(maxMorselHeapUsage, morsel.estimatedHeapUsage)
          memoryTracker.deallocated(totalTopHeapUsage)
          totalTopHeapUsage = maxMorselHeapUsage * morselCount
          memoryTracker.allocated(totalTopHeapUsage)
        }
        morsel.moveToNextRow()
      }
    }

    override def deallocateMemory(): Unit = {
      memoryTracker.deallocated(totalTopHeapUsage)
    }

    override protected def getTopRows: util.Iterator[PipelinedExecutionContext] = {
      topTable.sort()
      topTable.iterator()
    }
  }

  class ConcurrentTopTable(override val argumentRowId: Long,
                           override val argumentRowIdsForReducers: Array[Long],
                           comparator: Comparator[PipelinedExecutionContext],
                           limit: Int) extends TopTable {

    private val topTableByThread = new ConcurrentHashMap[Long, DefaultComparatorTopTable[PipelinedExecutionContext]]

    override def update(morsel: PipelinedExecutionContext): Unit = {
      val threadId = Thread.currentThread().getId
      val topTable = topTableByThread.computeIfAbsent(threadId, _ => new DefaultComparatorTopTable(comparator, limit))
      while (morsel.isValidRow) {
        topTable.add(morsel.shallowCopy())
        morsel.moveToNextRow()
      }
    }

    override def deallocateMemory(): Unit = {}

    override protected def getTopRows: util.Iterator[PipelinedExecutionContext] = {
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

