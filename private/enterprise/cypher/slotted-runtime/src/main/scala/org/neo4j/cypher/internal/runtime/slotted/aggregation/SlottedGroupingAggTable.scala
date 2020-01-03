/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.{AggregationTable, AggregationTableFactory}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{AggregationFunction, GroupingAggTable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ExecutionContextFactory, Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext

import scala.collection.JavaConverters._

/**
  * Slotted variant of [[GroupingAggTable]]
  */
class SlottedGroupingAggTable(slots: SlotConfiguration,
                              groupingColumns: GroupingExpression,
                              aggregations: Map[Int, AggregationExpression],
                              state: QueryState) extends AggregationTable {

  private var resultMap: java.util.LinkedHashMap[groupingColumns.KeyType, Array[AggregationFunction]] = _
  private val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }

  override def clear(): Unit = {
    resultMap = new java.util.LinkedHashMap[groupingColumns.KeyType, Array[AggregationFunction]]()
  }

  override def processRow(row: ExecutionContext): Unit = {
    val groupingValue = groupingColumns.computeGroupingKey(row, state)
    val functions = resultMap.computeIfAbsent(groupingValue, _ => {
      state.memoryTracker.allocated(groupingValue)
      val functions = new Array[AggregationFunction](aggregationExpressions.length)
      var i = 0
      while (i < aggregationExpressions.length) {
        functions(i) = aggregationExpressions(i).createAggregationFunction
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

  override def result(): Iterator[ExecutionContext] = {
    val innerIterator = resultMap.entrySet().iterator()
    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = innerIterator.hasNext

      override def next(): ExecutionContext = {
        val entry = innerIterator.next()
        val unorderedGroupingValue = entry.getKey
        val aggregateFunctions = entry.getValue
        val row = SlottedExecutionContext(slots)
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
    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory): AggregationTable =
      new SlottedGroupingAggTable(slots, groupingColumns, aggregations, state)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      aggregations.values.foreach(_.registerOwningPipe(pipe))
      groupingColumns.registerOwningPipe(pipe)
    }
  }

}
