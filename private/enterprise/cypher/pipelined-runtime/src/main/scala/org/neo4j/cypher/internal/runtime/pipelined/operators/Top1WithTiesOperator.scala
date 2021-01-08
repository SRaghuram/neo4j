/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.Top1WithTiesOperator.Top1WithTiesTable
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

/**
 * Reducing operator which collects pre-sorted input morsels until it
 * has seen all, and then streams the top tied rows.
 * Like all reducing operators, [[Top1WithTiesOperator]]
 * collects and streams grouped by argument rows ids.
 */
case class Top1WithTiesOperator(workIdentity: WorkIdentity,
                                orderBy: Seq[ColumnOrder]) {

  override def toString: String = "Top1WithTiesMerge"

  private val comparator: Comparator[ReadableRow] = MorselSorting.createComparator(orderBy)

  def mapper(argumentSlotOffset: Int, outputBufferId: BufferId): Top1WithTiesMapperOperator =
    new Top1WithTiesMapperOperator(argumentSlotOffset, outputBufferId)

  def reducer(argumentStateMapId: ArgumentStateMapId, operatorId: Id): Top1WithTiesReduceOperator =
    new Top1WithTiesReduceOperator(argumentStateMapId)(operatorId)

  /**
   * Pre-operator for pre-sorting and limiting morsels. The operator performs local sorting/limiting of the
   * data in a single morsel, before putting these locally processed morsels into the
   * [[ExecutionState]] buffer which writes the rows into a top table.
   */
  class Top1WithTiesMapperOperator(argumentSlotOffset: Int,
                                   outputBufferId: BufferId) extends OutputOperator {

    override def workIdentity: WorkIdentity = Top1WithTiesOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[Morsel]]](outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[Morsel]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = Top1WithTiesOperator.this.workIdentity
      override def trackTime: Boolean = true

      override def prepareOutput(morsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreTop1WithTiesOutput = {
        val perArguments = ArgumentStateMap.map(argumentSlotOffset, morsel, argumentMorsel => argumentMorsel)

        new PreTop1WithTiesOutput(perArguments, sink)
      }
    }

    class PreTop1WithTiesOutput(preTopped: IndexedSeq[PerArgument[Morsel]],
                                sink: Sink[IndexedSeq[PerArgument[Morsel]]]) extends PreparedOutput {
      override def produce(resources: QueryResources): Unit = {
        sink.put(preTopped, resources)
      }
    }
  }

  /**
   * Operator which streams sorted and limited data, built by [[Top1WithTiesMapperOperator]] and [[Top1WithTiesTable]].
   */
  class Top1WithTiesReduceOperator(argumentStateMapId: ArgumentStateMapId)
                                  (val id: Id = Id.INVALID_ID)
    extends Operator
    with AccumulatorsInputOperatorState[Morsel, Top1WithTiesTable] {

    override def workIdentity: WorkIdentity = Top1WithTiesOperator.this.workIdentity

    override def accumulatorsPerTask(morselSize: Int): Int = 1

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): AccumulatorsInputOperatorState[Morsel, Top1WithTiesTable] = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      argumentStateCreator.createArgumentStateMap(
        argumentStateMapId,
        new Top1WithTiesOperator.Factory(comparator),
        memoryTracker
      )
      this
    }

    override def nextTasks(state: PipelinedQueryState,
                           input: IndexedSeq[Top1WithTiesTable],
                           resources: QueryResources,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithAccumulators[Morsel, Top1WithTiesTable]] = {
      singletonIndexedSeq(new OTask(input))
    }

    class OTask(override val accumulators: IndexedSeq[Top1WithTiesTable]) extends ContinuableOperatorTaskWithAccumulators[Morsel, Top1WithTiesTable] {

      override def workIdentity: WorkIdentity = Top1WithTiesOperator.this.workIdentity

      override def toString: String = "Top1WithTiesMergeTask"

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

object Top1WithTiesOperator {

  class Factory(comparator: Comparator[ReadableRow]) extends ArgumentStateFactory[Top1WithTiesTable] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker): Top1WithTiesTable =
        new StandardTop1WithTiesTable(argumentRowId, argumentRowIdsForReducers, memoryTracker, comparator)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): Top1WithTiesTable =
        new ConcurrentTop1WithTiesTable(argumentRowId, argumentRowIdsForReducers, comparator)
  }

  /**
   * MorselAccumulator which returns top rows
   */
  abstract class Top1WithTiesTable extends MorselAccumulator[Morsel] {
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

  class StandardTop1WithTiesTable(override val argumentRowId: Long,
                                  override val argumentRowIdsForReducers: Array[Long],
                                  memoryTracker: MemoryTracker,
                                  comparator: Comparator[ReadableRow]) extends Top1WithTiesTable {
    private var rows: HeapTrackingArrayList[MorselRow] = _

    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      val cursor: MorselReadCursor = morsel.readCursor()
      while (cursor.next() ) {
        if (rows == null) {
          rows = HeapTrackingCollections.newArrayList(memoryTracker)
          rows.add(cursor.snapshot())
        } else {
          val comparison = comparator.compare(cursor, currentTop)
          if (comparison < 0) {
            rows.clear()
            rows.add(cursor.snapshot())
          }
          if (comparison == 0) {
            rows.add(cursor.snapshot())
          }
        }
      }
    }

    override def close(): Unit = {
      if  (rows != null) {
        rows.close()
        rows = null
      }
    }

    override protected def getTopRows: util.Iterator[MorselRow] = {
      if (rows == null) util.Collections.emptyIterator() else rows.iterator()
    }

    //CAVEAT: assumes rows is null-checked and contains at least one element
    private def currentTop: MorselRow = rows.get(0)
    override final def shallowSize: Long = StandardTop1WithTiesTable.SHALLOW_SIZE
  }

  object StandardTop1WithTiesTable {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[StandardTop1WithTiesTable])
  }

  class ConcurrentTop1WithTiesTable(override val argumentRowId: Long,
                                    override val argumentRowIdsForReducers: Array[Long],
                                    comparator: Comparator[ReadableRow]) extends Top1WithTiesTable {

    private val topByThread = new ConcurrentHashMap[Long, util.ArrayList[MorselRow]]

    override def update(morsel: Morsel, resources: QueryResources): Unit = {
      val threadId = Thread.currentThread().getId
      val topList = topByThread.computeIfAbsent(threadId, _ => new util.ArrayList[MorselRow]())
      val cursor: MorselReadCursor = morsel.readCursor()
      while (cursor.next()) {
        if (topList.isEmpty) {
          topList.add(cursor.snapshot())
        } else {
          val comparison = comparator.compare(cursor, topList.get(0))
          if (comparison < 0) {
            topList.clear()
            topList.add(cursor.snapshot())
          }
          if (comparison == 0) {
            topList.add(cursor.snapshot())
          }
        }
      }
    }

    override protected def getTopRows: util.Iterator[MorselRow] = {
      val topLists = topByThread.values().iterator()
      if (!topLists.hasNext) {
        util.Collections.emptyIterator()
      } else {
        val mergedTopList = topLists.next()
        while (topLists.hasNext) {
          val nextList = topLists.next()
          val comparison = comparator.compare(nextList.get(0), mergedTopList.get(0))
          if (comparison < 0) {
            mergedTopList.clear()
            mergedTopList.addAll(nextList)
          }
          if (comparison == 0) {
            mergedTopList.addAll(nextList)
          }
        }
        mergedTopList.iterator()
      }
    }

    override final def shallowSize: Long = ConcurrentTop1WithTiesTable.SHALLOW_SIZE
  }

  object ConcurrentTop1WithTiesTable {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[ConcurrentTop1WithTiesTable])
  }
}




