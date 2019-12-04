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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.OrderedGroupingAggTable

/**
  * Slotted variant of [[OrderedGroupingAggTable]]
  */
class SlottedOrderedGroupingAggTable(slots: SlotConfiguration,
                                     orderedGroupingColumns: GroupingExpression,
                                     unorderedGroupingColumns: GroupingExpression,
                                     aggregations: Map[Int, AggregationExpression],
                                     state: QueryState)
  extends SlottedGroupingAggTable(slots, unorderedGroupingColumns, aggregations, state) with OrderedChunkReceiver {

  private var currentGroupKey: orderedGroupingColumns.KeyType = _

  override def clear(): Unit = {
    currentGroupKey = null.asInstanceOf[orderedGroupingColumns.KeyType]
    super.clear()
  }

  override def isSameChunk(first: ExecutionContext,
                           current: ExecutionContext): Boolean = {
    if (currentGroupKey == null) {
      currentGroupKey = orderedGroupingColumns.computeGroupingKey(first, state)
    }
    current.eq(first) || currentGroupKey == orderedGroupingColumns.computeGroupingKey(current, state)
  }

  override def result(): Iterator[ExecutionContext] = {
    super.result().map { row =>
      orderedGroupingColumns.project(row, currentGroupKey)
      row
    }
  }

  override def processNextChunk: Boolean = true
}

object SlottedOrderedGroupingAggTable {
  case class Factory(slots: SlotConfiguration,
                     orderedGroupingColumns: GroupingExpression,
                     unorderedGroupingColumns: GroupingExpression,
                     aggregations: Map[Int, AggregationExpression]) extends OrderedAggregationTableFactory {

    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory): AggregationTable with OrderedChunkReceiver =
      new SlottedOrderedGroupingAggTable(slots, orderedGroupingColumns,unorderedGroupingColumns, aggregations, state)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      orderedGroupingColumns.registerOwningPipe(pipe)
      unorderedGroupingColumns.registerOwningPipe(pipe)
      aggregations.values.foreach(_.registerOwningPipe(pipe))
    }
  }
}
