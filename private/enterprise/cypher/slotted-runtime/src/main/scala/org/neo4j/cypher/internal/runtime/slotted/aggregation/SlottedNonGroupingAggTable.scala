/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExecutionContextFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

/**
 * Slotted variant of [[NonGroupingAggTable]]
 */
class SlottedNonGroupingAggTable(slots: SlotConfiguration,
                                 aggregations: Map[Int, AggregationExpression],
                                 state: QueryState,
                                 operatorId: Id) extends AggregationTable {
  private val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }
  private val aggregationFunctions = new Array[AggregationFunction](aggregationExpressions.length)
  private val scopedMemoryTracker: MemoryTracker = state.memoryTracker.memoryTrackerForOperator(operatorId.x).getScopedMemoryTracker

  override def clear(): Unit = {
    scopedMemoryTracker.reset()
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i) = aggregationExpressions(i).createAggregationFunction(scopedMemoryTracker)
      i += 1
    }
  }

  override def processRow(row: CypherRow): Unit = {
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i)(row, state)
      i += 1
    }
  }

  override def result(): Iterator[CypherRow] = {
    val row = resultRow()
    scopedMemoryTracker.close()
    Iterator.single(row)
  }

  protected def resultRow(): CypherRow = {
    val row = SlottedRow(slots)
    if (state.initialContext.nonEmpty) {
      state.initialContext.get.copyTo(row)
    }
    var i = 0
    while (i < aggregationFunctions.length) {
      row.setRefAt(aggregationOffsets(i), aggregationFunctions(i).result(state))
      i += 1
    }
    row
  }
}

object SlottedNonGroupingAggTable {

  case class Factory(slots: SlotConfiguration,
                     aggregations: Map[Int, AggregationExpression]) extends AggregationTableFactory {

    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory, operatorId: Id): AggregationTable =
      new SlottedNonGroupingAggTable(slots, aggregations, state, operatorId)
  }

}
