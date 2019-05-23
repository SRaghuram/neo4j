/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import java.util

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.{AggregationTable, AggregationTableFactory}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{AggregationFunction, GroupingAggTable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{AggregationPipe, ExecutionContextFactory, Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.values.storable.{LongArray, Values}

import scala.collection.JavaConverters._

/**
  * Slotted variant of [[GroupingAggTable]] when we have only primitive (nodes or relationships) grouping columns.
  */
class SlottedPrimitiveGroupingAggTable(slots: SlotConfiguration,
                                       readGrouping: Array[Int], // Offsets into the long array of the current execution context
                                       writeGrouping: Array[Int], // Offsets into the long array of the current execution context
                                       aggregations: Map[Int, AggregationExpression],
                                       state: QueryState) extends AggregationTable {

  protected val resultMap = new util.LinkedHashMap[LongArray, Array[AggregationFunction]]()
  private val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }

  private def computeGroupingKey(row: ExecutionContext): LongArray = {
    val keys = new Array[Long](readGrouping.length)
    var i = 0
    while (i < readGrouping.length) {
      keys(i) = row.getLongAt(readGrouping(i))
      i += 1
    }
    Values.longArray(keys)
  }

  private def projectGroupingKey(ctx: ExecutionContext, key: LongArray): Unit = {
    var i = 0
    while (i < writeGrouping.length) {
      ctx.setLongAt(writeGrouping(i), key.longValue(i))
      i += 1
    }
  }

  private def createResultRow(groupingKey: LongArray, aggregateFunctions: Seq[AggregationFunction]): ExecutionContext = {
    val row = SlottedExecutionContext(slots)
    projectGroupingKey(row, groupingKey)
    var i = 0
    while (i < aggregateFunctions.length) {
      row.setRefAt(aggregationOffsets(i), aggregateFunctions(i).result(state))
      i += 1
    }
    row
  }

  override def clear(): Unit = {
    resultMap.clear()
  }

  override def processRow(row: ExecutionContext): Unit = {
    val groupingValue = computeGroupingKey(row)
    val functions = resultMap.computeIfAbsent(groupingValue, _ => aggregationExpressions.map(_.createAggregationFunction))
    functions.foreach(func => func(row, state))
  }

  override def result(): Iterator[ExecutionContext] = {
    resultMap.entrySet().iterator().asScala.map {
      e: java.util.Map.Entry[LongArray, Array[AggregationFunction]] => createResultRow(e.getKey, e.getValue)
    }
  }
}

object SlottedPrimitiveGroupingAggTable {

  case class Factory(slots: SlotConfiguration,
                     readGrouping: Array[Int],
                     writeGrouping: Array[Int],
                     aggregations: Map[Int, AggregationExpression]) extends AggregationTableFactory {
    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory): AggregationPipe.AggregationTable =
      new SlottedPrimitiveGroupingAggTable(slots, readGrouping, writeGrouping, aggregations, state)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      aggregations.values.foreach(_.registerOwningPipe(pipe))
    }
  }

}
