/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.physicalplanning.{BufferId, PipelineId}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.zombie.ExecutionState
import org.neo4j.cypher.internal.runtime.zombie.aggregators.{Aggregator, Updater}
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Sink
import org.neo4j.internal.kernel.api.IndexReadSession


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

    override def prepareOutput(morsel: MorselExecutionContext,
                               context: QueryContext,
                               state: QueryState,
                               resources: QueryResources): PreAggregatedOutput = {

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
