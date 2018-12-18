/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.vectorized.expressions.AggregationReducer

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
    private val incomingSlots = aggregations.map(_.mapperOutputSlot)
    private val outgoingSlots = aggregations.map(_.reducerOutputSlot)
    private var aggregates: Iterator[(GroupingKey, Array[AggregationReducer])] = _

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      if (null == aggregates) {
        aggregates = aggregateInputs(inputMorsels)
      }
      while (aggregates.hasNext && outputRow.hasMoreRows) {
        val (key, reducers) = aggregates.next()
        groupings.project(outputRow, key)
        var i = 0
        while (i < aggregations.length) {
          val reducer = reducers(i)
          outputRow.setRefAt(outgoingSlots(i), reducer.result)
          i += 1
        }
        outputRow.moveToNextRow()
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = aggregates.hasNext

    private def aggregateInputs(inputMorsels: Array[MorselExecutionContext]) = {

      var morselPos = 0
      val result =  mutable.LinkedHashMap[GroupingKey, Array[AggregationReducer]]()
      while (morselPos < inputMorsels.length) {
        val currentIncomingRow = inputMorsels(morselPos)
        while (currentIncomingRow.hasMoreRows) {
          val key = groupings.getGroupingKey(currentIncomingRow)
          val reducersForKey = result.getOrElseUpdate(key, aggregations.map(_.aggregation.createAggregationReducer))
          var i = 0
          while (i < aggregations.length) {
            val reducer = reducersForKey(i)
            reducer.reduce(currentIncomingRow.getRefAt(incomingSlots(i)))
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
