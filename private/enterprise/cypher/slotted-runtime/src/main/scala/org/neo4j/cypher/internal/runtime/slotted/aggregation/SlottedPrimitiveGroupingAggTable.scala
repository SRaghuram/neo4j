/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.eclipse.collections.api.block.function.Function2
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.computeNewAggregatorsFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.GroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * Slotted variant of [[GroupingAggTable]] when we have only primitive (nodes or relationships) grouping columns.
 */
class SlottedPrimitiveGroupingAggTable(slots: SlotConfiguration,
                                       readGrouping: Array[Int], // Offsets into the long array of the current execution context
                                       writeGrouping: Array[Int], // Offsets into the long array of the current execution context
                                       aggregations: Map[Int, AggregationExpression],
                                       state: QueryState,
                                       operatorId: Id) extends AggregationTable {

  private[this] var resultMap: HeapTrackingOrderedAppendMap[LongArray, Array[AggregationFunction]] = _
  private[this] val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }
  private[this] val memoryTracker = state.memoryTracker.memoryTrackerForOperator(operatorId.x)

  private[this] val newAggregators: Function2[LongArray, MemoryTracker, Array[AggregationFunction]] =
    computeNewAggregatorsFunction(aggregationExpressions)

  private def computeGroupingKey(row: CypherRow): LongArray = {
    val keys = new Array[Long](readGrouping.length)
    var i = 0
    while (i < readGrouping.length) {
      keys(i) = row.getLongAt(readGrouping(i))
      i += 1
    }
    Values.longArray(keys)
  }

  private def projectGroupingKey(ctx: CypherRow, key: LongArray): Unit = {
    var i = 0
    while (i < writeGrouping.length) {
      ctx.setLongAt(writeGrouping(i), key.longValue(i))
      i += 1
    }
  }

  private def createResultRow(groupingKey: LongArray, aggregateFunctions: Seq[AggregationFunction]): CypherRow = {
    val row = SlottedRow(slots)
    if (state.initialContext.nonEmpty) {
      row.copyAllFrom(state.initialContext.get)
    }
    projectGroupingKey(row, groupingKey)
    var i = 0
    while (i < aggregateFunctions.length) {
      row.setRefAt(aggregationOffsets(i), aggregateFunctions(i).result(state))
      i += 1
    }
    row
  }

  override def clear(): Unit = {
    if (resultMap != null) {
      resultMap.close()
    }
    resultMap = HeapTrackingOrderedAppendMap.createOrderedMap[LongArray, Array[AggregationFunction]](memoryTracker)
    state.query.resources.trace(resultMap)
  }

  override def processRow(row: CypherRow): Unit = {
    val groupingValue = computeGroupingKey(row)
    val functions = resultMap.getIfAbsentPutWithMemoryTracker2(groupingValue, newAggregators)
    var i = 0
    while (i < functions.length) {
      functions(i)(row, state)
      i += 1
    }
  }

  override def result(): ClosingIterator[CypherRow] = {
    ClosingIterator(resultMap.autoClosingEntryIterator.asScala).map {
      e: java.util.Map.Entry[LongArray, Array[AggregationFunction]] => createResultRow(e.getKey, e.getValue)
    }.closing(resultMap)
  }
}

object SlottedPrimitiveGroupingAggTable {

  case class Factory(slots: SlotConfiguration,
                     readGrouping: Array[Int],
                     writeGrouping: Array[Int],
                     aggregations: Map[Int, AggregationExpression]) extends AggregationTableFactory {
    override def table(state: QueryState, rowFactory: CypherRowFactory, operatorId: Id): AggregationPipe.AggregationTable =
      new SlottedPrimitiveGroupingAggTable(slots, readGrouping, writeGrouping, aggregations, state, operatorId)
  }

}
