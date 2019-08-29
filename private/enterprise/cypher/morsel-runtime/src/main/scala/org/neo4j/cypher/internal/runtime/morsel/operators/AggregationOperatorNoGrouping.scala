/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.{NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.aggregators.{AggregatingAccumulator, Aggregator, Updater}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, StateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.morsel.{ArgumentStateMapCreator, ExecutionState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.internal.kernel.api.IndexReadSession

case class AggregationOperatorNoGrouping(workIdentity: WorkIdentity,
                                         aggregations: Array[Aggregator]) {

  def mapper(argumentSlotOffset: Int,
             outputBufferId: BufferId,
             expressionValues: Array[Expression]) =
    new AggregationMapperOperatorNoGrouping(workIdentity,
                                            argumentSlotOffset,
                                            outputBufferId,
                                            aggregations,
                                            expressionValues)

  def reducer(argumentStateMapId: ArgumentStateMapId,
              reducerOutputSlots: Array[Int]) =
    new AggregationReduceOperatorNoGrouping(argumentStateMapId,
                                            workIdentity,
                                            aggregations,
                                            reducerOutputSlots)

  // =========== THE MAPPER ============

  /**
    * Pre-operator for aggregations with no grouping. This performs local aggregation of the
    * data in a single morsel at a time, before putting these local aggregations into the
    * [[ExecutionState]] buffer which perform the final global aggregation.
    */
  class AggregationMapperOperatorNoGrouping(val workIdentity: WorkIdentity,
                                            argumentSlotOffset: Int,
                                            outputBufferId: BufferId,
                                            aggregations: Array[Aggregator],
                                            expressionValues: Array[Expression]) extends OutputOperator {

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState,
                             pipelineId: PipelineId): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[Array[Updater]]]](pipelineId, outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[Array[Updater]]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = AggregationMapperOperatorNoGrouping.this.workIdentity

      override def prepareOutput(morsel: MorselExecutionContext,
                                 context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreAggregatedOutput = {

        val queryState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)

        val preAggregated = ArgumentStateMap.map(argumentSlotOffset,
                                                 morsel,
                                                 preAggregate(queryState))

        new PreAggregatedOutput(preAggregated, sink)
      }

      private def preAggregate(queryState: OldQueryState)(morsel: MorselExecutionContext): Array[Updater] = {
        val updaters = aggregations.map(_.newUpdater)
        //loop over the entire morsel view and apply the aggregation
        while (morsel.isValidRow) {
          var i = 0
          while (i < aggregations.length) {
            val value = expressionValues(i)(morsel, queryState)
            updaters(i).update(value)
            i += 1
          }
          morsel.moveToNextRow()
        }
        updaters
      }
    }

    class PreAggregatedOutput(preAggregated: IndexedSeq[PerArgument[Array[Updater]]],
                              sink: Sink[IndexedSeq[PerArgument[Array[Updater]]]]) extends PreparedOutput {
      override def produce(): Unit = sink.put(preAggregated)
    }
  }

  // =========== THE REDUCER ============

  /**
    * Operator which streams aggregated data, built by [[AggregationMapperOperatorNoGrouping]] and [[AggregatingAccumulator]].
    */
  class AggregationReduceOperatorNoGrouping(val argumentStateMapId: ArgumentStateMapId,
                                            val workIdentity: WorkIdentity,
                                            aggregations: Array[Aggregator],
                                            reducerOutputSlots: Array[Int])
    extends Operator
      with ReduceOperatorState[Array[Updater], AggregatingAccumulator] {

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             queryContext: QueryContext,
                             state: QueryState,
                             resources: QueryResources): ReduceOperatorState[Array[Updater], AggregatingAccumulator] = {
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations, stateFactory.memoryTracker))
      this
    }

    override def nextTasks(queryContext: QueryContext,
                           state: QueryState,
                           input: AggregatingAccumulator,
                           resources: QueryResources
                          ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[Array[Updater], AggregatingAccumulator]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulator: AggregatingAccumulator) extends ContinuableOperatorTaskWithAccumulator[Array[Updater], AggregatingAccumulator] {

      override def workIdentity: WorkIdentity = AggregationReduceOperatorNoGrouping.this.workIdentity

      override def operate(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState,
                           resources: QueryResources): Unit = {

        var i = 0
        while (i < aggregations.length) {
          outputRow.setRefAt(reducerOutputSlots(i), accumulator.result(i))
          i += 1
        }
        outputRow.moveToNextRow()
        outputRow.finishedWriting()
      }

      // This operator will never continue since it will always write a single row
      override def canContinue: Boolean = false
    }
  }
}

