/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.internal.kernel.api.IndexReadSession

import scala.collection.mutable

/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperator(val workIdentity: WorkIdentity,
                                aggregations: Array[AggregationOffsets],
                                groupings: GroupingExpression) extends EagerReduceOperator {
  private type GroupingKey = groupings.KeyType

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsels: Seq[MorselExecutionContext],
                    resources: QueryResources): ContinuableOperatorTask = {
    new OTask(inputMorsels.toArray)
  }

  class OTask(inputMorsels: Array[MorselExecutionContext]) extends ContinuableOperatorTask {
    private val outgoingSlots = aggregations.map(_.reducerOutputSlot)
    private var aggregates: Iterator[(GroupingKey, Array[AggregationFunction])] = _

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      val queryState = new OldQueryState(context,
        resources = null,
        params = state.params,
        resources.expressionCursors,
        Array.empty[IndexReadSession])

      if (null == aggregates) {
        aggregates = aggregateInputs(inputMorsels, queryState)
      }
      while (aggregates.hasNext && outputRow.isValidRow) {
        val (key, reducers) = aggregates.next()
        groupings.project(outputRow, key)
        var i = 0
        while (i < aggregations.length) {
          val reducer = reducers(i)
          outputRow.setRefAt(outgoingSlots(i), reducer.result(queryState))
          i += 1
        }
        outputRow.moveToNextRow()
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = aggregates.hasNext

    private def aggregateInputs(inputMorsels: Array[MorselExecutionContext], queryState: OldQueryState): Iterator[(GroupingKey, Array[AggregationFunction])] = {

      var morselPos = 0
      val result =  mutable.LinkedHashMap[GroupingKey, Array[AggregationFunction]]()
      while (morselPos < inputMorsels.length) {
        val currentIncomingRow = inputMorsels(morselPos)
        while (currentIncomingRow.isValidRow) {
          val key = groupings.getGroupingKey(currentIncomingRow)
          val reducersForKey = result.getOrElseUpdate(key, aggregations.map(_.createReducer))
          var i = 0
          while (i < aggregations.length) {
            val reducer = reducersForKey(i)
            reducer.apply(currentIncomingRow, queryState)
            i += 1
          }
          currentIncomingRow.moveToNextRow()
        }
        morselPos += 1
      }
      result.iterator
    }
  }
}
