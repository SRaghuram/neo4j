/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.DefaultComparatorTopTable
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.TopOperator.TopTable
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator, PerArgument}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, StateFactory}
import org.neo4j.cypher.internal.runtime.morsel.{ArgumentStateMapCreator, ExecutionState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryMemoryTracker, WithHeapUsageEstimation}

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

  private val comparator: Comparator[MorselExecutionContext] = MorselSorting.createComparator(orderBy)

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
      new State(executionState.getSink[IndexedSeq[PerArgument[MorselExecutionContext]]](pipelineId, outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[MorselExecutionContext]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

      override def prepareOutput(morsel: MorselExecutionContext,
                                 context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreTopOutput = {

        val preTopped = ArgumentStateMap.map(argumentSlotOffset, morsel, preTop)
        new PreTopOutput(preTopped, sink)
      }

      private def preTop(morsel: MorselExecutionContext): MorselExecutionContext = {
        morsel
        // TODO sort & compact the morsel
      }
    }

    class PreTopOutput(preTopped: IndexedSeq[PerArgument[MorselExecutionContext]],
                       sink: Sink[IndexedSeq[PerArgument[MorselExecutionContext]]]) extends PreparedOutput {
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
    with ReduceOperatorState[MorselExecutionContext, TopTable] {

    override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             queryContext: QueryContext,
                             state: QueryState,
                             resources: QueryResources): ReduceOperatorState[MorselExecutionContext, TopTable] = {
      // TODO note: limit is only allowed to be an Int for top table. do this safely
      val limit = LimitOperator.evaluateCountValue(queryContext, state, resources, countExpression).toInt
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new TopOperator.Factory(stateFactory.memoryTracker, comparator, limit))
      this
    }

    override def nextTasks(queryContext: QueryContext,
                           state: QueryState,
                           input: TopTable,
                           resources: QueryResources
                          ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[MorselExecutionContext, TopTable]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulator: TopTable) extends ContinuableOperatorTaskWithAccumulator[MorselExecutionContext, TopTable] {

      override def workIdentity: WorkIdentity = TopOperator.this.workIdentity

      override def toString: String = "TopMergeTask"

      var sortedInputPerArgument: util.Iterator[MorselExecutionContext] = _

      override def operate(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState,
                           resources: QueryResources): Unit = {

        if (sortedInputPerArgument == null) {
          sortedInputPerArgument = accumulator.topRows()
        }

        while (outputRow.isValidRow && canContinue) {
          val nextRow: MorselExecutionContext = sortedInputPerArgument.next()
          outputRow.copyFrom(nextRow)
          nextRow.moveToNextRow()
          outputRow.moveToNextRow()
        }

        outputRow.finishedWriting()
      }

      override def canContinue: Boolean = sortedInputPerArgument.hasNext
    }

  }

}

object TopOperator {

  class Factory(memoryTracker: QueryMemoryTracker, comparator: Comparator[MorselExecutionContext], limit: Int) extends ArgumentStateFactory[TopTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): TopTable =
      new StandardTopTable(argumentRowId, argumentRowIdsForReducers, memoryTracker, comparator, limit)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): TopTable =
      new ConcurrentTopTable(argumentRowId, argumentRowIdsForReducers, memoryTracker, comparator, limit)
  }

  /**
   * MorselAccumulator which returns top `limit` rows
   */
  abstract class TopTable extends MorselAccumulator[MorselExecutionContext] {
    private var sorted = false

    final def topRows(): java.util.Iterator[MorselExecutionContext] = {
      if (sorted) {
        throw new IllegalArgumentException("Method should not be called more than once, per top table instance")
      }
      val rows = getTopRows
      sorted = true
      rows
    }

    protected def getTopRows: java.util.Iterator[MorselExecutionContext]
  }

  class StandardTopTable(override val argumentRowId: Long,
                         override val argumentRowIdsForReducers: Array[Long],
                         memoryTracker: QueryMemoryTracker,
                         comparator: Comparator[MorselExecutionContext],
                         limit: Int) extends TopTable with WithHeapUsageEstimation {

    private val topTable = new DefaultComparatorTopTable(comparator, limit)

    override def update(morsel: MorselExecutionContext): Unit = {
      while (morsel.isValidRow) {
        topTable.add(morsel.shallowCopy())
        morsel.moveToNextRow()
      }
    }

    override protected def getTopRows: util.Iterator[MorselExecutionContext] = {
      topTable.sort()
      topTable.iterator()
    }

    // TODO track heap usage
    override def estimatedHeapUsage: Long = ???
  }

  class ConcurrentTopTable(override val argumentRowId: Long,
                           override val argumentRowIdsForReducers: Array[Long],
                           memoryTracker: QueryMemoryTracker,
                           comparator: Comparator[MorselExecutionContext],
                           limit: Int) extends TopTable with WithHeapUsageEstimation {

    private val topTableByThread = new ConcurrentHashMap[Long, DefaultComparatorTopTable[MorselExecutionContext]]

    override def update(morsel: MorselExecutionContext): Unit = {
      val threadId = Thread.currentThread().getId
      val topTable = topTableByThread.computeIfAbsent(threadId, _ => new DefaultComparatorTopTable(comparator, limit))
      while (morsel.isValidRow) {
        topTable.add(morsel.shallowCopy())
        morsel.moveToNextRow()
      }
    }

    override protected def getTopRows: util.Iterator[MorselExecutionContext] = {
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

    // TODO track heap usage
    override def estimatedHeapUsage: Long = ???
  }

}

