/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExecutionContextFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.GroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Slotted variant of [[GroupingAggTable]]
 */
class SlottedGroupingAggTable(slots: SlotConfiguration,
                              groupingColumns: GroupingExpression,
                              aggregations: Map[Int, AggregationExpression],
                              state: QueryState,
                              operatorId: Id) extends AggregationTable {

  private var resultMap: java.util.LinkedHashMap[groupingColumns.KeyType, Array[AggregationFunction]] = _
  private val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }

  override def clear(): Unit = {
    if (resultMap != null) {
      resultMap.forEach { (key, functions) =>
        state.memoryTracker.deallocated(key, operatorId.x)
        functions.foreach(_.recordMemoryDeallocation(state))
      }
    }
    resultMap = new java.util.LinkedHashMap[groupingColumns.KeyType, Array[AggregationFunction]]()
  }

  override def processRow(row: CypherRow): Unit = {
    val groupingValue = groupingColumns.computeGroupingKey(row, state)
    val functions = resultMap.computeIfAbsent(groupingValue, _ => {
      state.memoryTracker.allocated(groupingValue, operatorId.x)
      val functions = new Array[AggregationFunction](aggregationExpressions.length)
      var i = 0
      while (i < aggregationExpressions.length) {
        functions(i) = aggregationExpressions(i).createAggregationFunction(operatorId)
        i += 1
      }
      functions
    })
    var i = 0
    while (i < functions.length) {
      functions(i)(row, state)
      i += 1
    }
  }

  override def result(): Iterator[CypherRow] = {
    val innerIterator = resultMap.entrySet().iterator()
    new Iterator[CypherRow] {
      override def hasNext: Boolean = innerIterator.hasNext

      override def next(): CypherRow = {
        val entry = innerIterator.next()
        val unorderedGroupingValue = entry.getKey
        val aggregateFunctions = entry.getValue
        val row = SlottedRow(slots)
        if (state.initialContext.nonEmpty) {
          state.initialContext.get.copyTo(row)
        }
        groupingColumns.project(row, unorderedGroupingValue)
        var i = 0
        while (i < aggregateFunctions.length) {
          row.setRefAt(aggregationOffsets(i), aggregateFunctions(i).result(state))
          i += 1
        }
        row
      }
    }
  }
}

object SlottedGroupingAggTable {

  case class Factory(slots: SlotConfiguration,
                     groupingColumns: GroupingExpression,
                     aggregations: Map[Int, AggregationExpression]) extends AggregationTableFactory {
    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory, operatorId: Id): AggregationTable =
      new SlottedGroupingAggTable(slots, groupingColumns, aggregations, state, operatorId)
  }

}
