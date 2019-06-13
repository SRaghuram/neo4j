/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.aggregators.{Aggregator, Reducer, Updater}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator, PerArgument}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.morsel.{ArgumentStateMapCreator, ExecutionState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.internal.kernel.api.IndexReadSession

/**
  * General purpose aggregation operator, supporting clauses like
  *
  * {{{
  *   WITH key1, key2, key3, sum(..) AS aggr1, count(..) AS aggr2, avg(..) AS aggr3
  * }}}
  *
  * The implementation composes an [[AggregationMapperOperator]], an [[AggregatingAccumulator]] and an [[AggregationReduceOperator]].
  */
case class AggregationOperator(workIdentity: WorkIdentity,
                               aggregations: Array[Aggregator],
                               groupings: GroupingExpression) {

  type AggPreMap = java.util.LinkedHashMap[groupings.KeyType, Array[Updater]]

  private val newUpdaters: java.util.function.Function[ groupings.KeyType, Array[Updater]] =
    (_: groupings.KeyType) => aggregations.map(_.newUpdater)

  def mapper(argumentSlotOffset: Int,
             outputBufferId: BufferId,
             expressionValues: Array[Expression]) =
    new AggregationMapperOperator(argumentSlotOffset, outputBufferId, expressionValues)

  def reducer(argumentStateMapId: ArgumentStateMapId,
              reducerOutputSlots: Array[Int]) =
    new AggregationReduceOperator(argumentStateMapId, reducerOutputSlots)

  /**
    * Pre-operator for aggregations with grouping. This performs local aggregation of the
    * data in a single morsel at a time, before putting these local aggregations into the
    * [[ExecutionState]] buffer which perform the final global aggregation.
    */
  class AggregationMapperOperator(argumentSlotOffset: Int,
                                  outputBufferId: BufferId,
                                  expressionValues: Array[Expression]) extends OutputOperator {

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState,
                             pipelineId: PipelineId): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[AggPreMap]]](pipelineId, outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[AggPreMap]]]) extends OutputOperatorState {

      override def prepareOutput(morsel: MorselExecutionContext,
                                 context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources,
                                 queryProfiler: QueryProfiler): PreAggregatedOutput = {

        val operatorProfileEvent = queryProfiler.executeOperator(workIdentity.workId)

        try {
          val queryState = new OldQueryState(context,
                                             resources = null,
                                             params = state.params,
                                             resources.expressionCursors,
                                             Array.empty[IndexReadSession],
                                             resources.expressionVariables(state.nExpressionSlots))

          val preAggregated = ArgumentStateMap.map(argumentSlotOffset,
                                               morsel,
                                               preAggregate(queryState))

          new PreAggregatedOutput(preAggregated, sink)
        } finally {
          operatorProfileEvent.close()
        }
      }

      private def preAggregate(queryState: OldQueryState)
                              (morsel: MorselExecutionContext): AggPreMap = {

        val result = new AggPreMap()

        //loop over the entire morsel view and apply the aggregation
        while (morsel.isValidRow) {
          val groupingValue = groupings.computeGroupingKey(morsel, queryState)
          val updaters = result.computeIfAbsent(groupingValue, newUpdaters)
          var i = 0
          while (i < aggregations.length) {
            val value = expressionValues(i)(morsel, queryState)
            updaters(i).update(value)
            i += 1
          }
          morsel.moveToNextRow()
        }
        result
      }
    }

    class PreAggregatedOutput(preAggregated: IndexedSeq[PerArgument[AggPreMap]],
                              sink: Sink[IndexedSeq[PerArgument[AggPreMap]]]) extends PreparedOutput {
      override def produce(): Unit = sink.put(preAggregated)
    }
  }

  /**
    * Accumulator that compacts input data using some [[Reducer]]s.
    */
  abstract class AggregatingAccumulator extends MorselAccumulator[AggPreMap] {
    /**
      * Return the result of the reducer.
      */
    def result(): java.util.Iterator[java.util.Map.Entry[groupings.KeyType, Array[Reducer]]]
  }

  class StandardAggregatingAccumulator(override val argumentRowId: Long,
                                       aggregators: Array[Aggregator]) extends AggregatingAccumulator {

    val reducerMap = new java.util.LinkedHashMap[groupings.KeyType, Array[Reducer]]

    override def update(data: AggPreMap): Unit = {
     val iterator = data.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val reducers = reducerMap.computeIfAbsent(entry.getKey, key => aggregators.map(_.newStandardReducer))
        var i = 0
        while (i < reducers.length) {
          reducers(i).update(entry.getValue()(i))
          i += 1
        }
      }
    }

    def result(): java.util.Iterator[java.util.Map.Entry[groupings.KeyType, Array[Reducer]]] = reducerMap.entrySet().iterator()
  }

  class ConcurrentAggregatingAccumulator(override val argumentRowId: Long,
                                         aggregators: Array[Aggregator]) extends AggregatingAccumulator {

    val reducerMap = new ConcurrentHashMap[groupings.KeyType, Array[Reducer]]

    override def update(data: AggPreMap): Unit = {
      val iterator = data.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
          val reducers = reducerMap.computeIfAbsent(entry.getKey, key => aggregators.map(_.newConcurrentReducer))
          var i = 0
          while (i < reducers.length) {
            reducers(i).update(entry.getValue()(i))
            i += 1
          }
      }
    }

    def result(): java.util.Iterator[java.util.Map.Entry[groupings.KeyType, Array[Reducer]]] = reducerMap.entrySet().iterator()
  }

  object AggregatingAccumulator {

    class Factory(aggregators: Array[Aggregator]) extends ArgumentStateFactory[AggregatingAccumulator] {
      override def newStandardArgumentState(argumentRowId: Long): AggregatingAccumulator =
        new StandardAggregatingAccumulator(argumentRowId, aggregators)

      override def newConcurrentArgumentState(argumentRowId: Long): AggregatingAccumulator =
        new ConcurrentAggregatingAccumulator(argumentRowId, aggregators)
    }
  }

  /**
    * Operator which streams aggregated data, built by [[AggregationMapperOperator]] and [[AggregatingAccumulator]].
    */
  class AggregationReduceOperator(val argumentStateMapId: ArgumentStateMapId,
                                  reducerOutputSlots: Array[Int])
    extends Operator
      with ReduceOperatorState[AggPreMap, AggregatingAccumulator] {

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def createState(argumentStateCreator: ArgumentStateMapCreator): ReduceOperatorState[AggPreMap, AggregatingAccumulator] = {
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations))
      this
    }

    override def nextTasks(queryContext: QueryContext,
                           state: QueryState,
                           input: AggregatingAccumulator,
                           resources: QueryResources
                          ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[AggPreMap, AggregatingAccumulator]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulator: AggregatingAccumulator)
      extends ContinuableOperatorTaskWithAccumulator[AggPreMap, AggregatingAccumulator] {

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      private val resultIterator = accumulator.result()

      override def operate(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState,
                           resources: QueryResources): Unit = {

        while (resultIterator.hasNext && outputRow.isValidRow) {
          val entry = resultIterator.next()
          val key = entry.getKey
          val reducers = entry.getValue

          groupings.project(outputRow, key)
          var i = 0
          while (i < aggregations.length) {
            outputRow.setRefAt(reducerOutputSlots(i), reducers(i).result)
            i += 1
          }
          outputRow.moveToNextRow()
        }
        outputRow.finishedWriting()
      }

      override def canContinue: Boolean = resultIterator.hasNext

      override protected def closeCursors(resources: QueryResources): Unit = {}
    }
  }
}
