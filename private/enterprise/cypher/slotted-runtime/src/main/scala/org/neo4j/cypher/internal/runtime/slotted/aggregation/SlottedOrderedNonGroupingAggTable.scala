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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExecutionContextFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedAggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedChunkReceiver
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.OrderedNonGroupingAggTable
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Slotted variant of [[OrderedNonGroupingAggTable]]
 */
class SlottedOrderedNonGroupingAggTable(slots: SlotConfiguration,
                                        orderedGroupingColumns: GroupingExpression,
                                        aggregations: Map[Int, AggregationExpression],
                                        state: QueryState,
                                        operatorId: Id)
  extends SlottedNonGroupingAggTable(slots, aggregations, state, operatorId) with OrderedChunkReceiver {

  private var currentGroupKey: orderedGroupingColumns.KeyType = _

  override def clear(): Unit = {
    currentGroupKey = null.asInstanceOf[orderedGroupingColumns.KeyType]
    super.clear()
  }

  override def isSameChunk(first: CypherRow,
                           current: CypherRow): Boolean = {
    if (currentGroupKey == null) {
      currentGroupKey = orderedGroupingColumns.computeGroupingKey(first, state)
    }
    current.eq(first) || currentGroupKey == orderedGroupingColumns.computeGroupingKey(current, state)
  }

  override def result(): Iterator[CypherRow] = {
    val row = resultRow()
    orderedGroupingColumns.project(row, currentGroupKey)
    Iterator.single(row)
  }

  override def processNextChunk: Boolean = true
}

object SlottedOrderedNonGroupingAggTable {
  case class Factory(slots: SlotConfiguration,
                     orderedGroupingColumns: GroupingExpression,
                     aggregations: Map[Int, AggregationExpression]) extends OrderedAggregationTableFactory {

    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory, operatorId: Id): AggregationTable with OrderedChunkReceiver =
      new SlottedOrderedNonGroupingAggTable(slots, orderedGroupingColumns, aggregations, state, operatorId)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      orderedGroupingColumns.registerOwningPipe(pipe)
      aggregations.values.foreach(_.registerOwningPipe(pipe))
    }
  }
}
