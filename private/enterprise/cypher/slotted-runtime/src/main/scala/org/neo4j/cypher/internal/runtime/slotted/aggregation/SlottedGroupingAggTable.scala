/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import scala.collection.mutable

/**
  * Slotted variant of [[GroupingAggTable]]
  */
class SlottedGroupingAggTable(slots: SlotConfiguration,
                              groupingColumns: GroupingExpression,
                              aggregations: Map[Int, AggregationExpression],
                              state: QueryState) extends AggregationTable {

  private val resultMap: mutable.LinkedHashMap[groupingColumns.KeyType, Array[AggregationFunction]] = mutable.LinkedHashMap[groupingColumns.KeyType, Array[AggregationFunction]]()
  private val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }

  override def clear(): Unit = {
    resultMap.clear()
  }

  override def processRow(row: ExecutionContext): Unit = {
    val groupingValue = groupingColumns.computeGroupingKey(row, state)
    val functions = resultMap.getOrElseUpdate(groupingValue, aggregationExpressions.map(_.createAggregationFunction))
    functions.foreach(func => func(row, state))
  }

  override def result(): Iterator[ExecutionContext] = {
    resultMap.toIterator.map {
      case (unorderedGroupingValue, aggregateFunctions) =>
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
