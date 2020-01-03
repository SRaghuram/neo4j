/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.{AggregationTable, AggregationTableFactory}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{AggregationFunction, NonGroupingAggTable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ExecutionContextFactory, Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext

/**
  * Slotted variant of [[NonGroupingAggTable]]
  */
class SlottedNonGroupingAggTable(slots: SlotConfiguration,
                                 aggregations: Map[Int, AggregationExpression],
                                 state: QueryState) extends AggregationTable {
  private val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }
  private val aggregationFunctions = new Array[AggregationFunction](aggregationExpressions.length)

  override def clear(): Unit = {
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i) = aggregationExpressions(i).createAggregationFunction
      i += 1
    }
  }

  override def processRow(row: ExecutionContext): Unit = {
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i)(row, state)
      i += 1
    }
  }

  override def result(): Iterator[ExecutionContext] = {
    Iterator.single(resultRow())
  }

  protected def resultRow(): ExecutionContext = {
    val row = SlottedExecutionContext(slots)
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

    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory): AggregationTable =
      new SlottedNonGroupingAggTable(slots, aggregations, state)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      aggregations.values.foreach(_.registerOwningPipe(pipe))
    }
  }

}
