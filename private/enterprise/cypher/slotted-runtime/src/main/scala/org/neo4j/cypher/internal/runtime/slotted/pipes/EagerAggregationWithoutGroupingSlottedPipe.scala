/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

/*
This pipe can be used whenever we are aggregating and not grouping on anything
 */
case class EagerAggregationWithoutGroupingSlottedPipe(source: Pipe,
                                                      slots: SlotConfiguration,
                                                      aggregations: Map[Int, AggregationExpression])
                                                     (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))

  private val (aggregationOffsets: IndexedSeq[Int], aggregationFunctions: IndexedSeq[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toIndexedSeq, b.toIndexedSeq)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    val aggregationAccumulators: Seq[AggregationFunction] = aggregationFunctions.map(_.createAggregationFunction)

    if (input.isEmpty)
      createEmptyResult(state)
    else {
      // Consume input
      input.foreach { ctx =>
        aggregationAccumulators.foreach(f => f.apply(ctx, state))
      }

      // Present result
      val context = SlottedExecutionContext(slots)
      (aggregationOffsets zip aggregationAccumulators).foreach {
        case (offset, value) => context.setRefAt(offset, value.result(state))
      }
      Iterator(context)
    }
  }

  // Used when we have no input and no grouping expressions. In this case, we'll return a single row
  def createEmptyResult(state: QueryState): Iterator[ExecutionContext] = {
    val context = SlottedExecutionContext(slots)
    val aggregationOffsetsAndFunctions = aggregationOffsets zip aggregations
      .map(_._2.createAggregationFunction.result(state))

    aggregationOffsetsAndFunctions.toMap.foreach {
      case (offset, zeroValue) => context.setRefAt(offset, zeroValue)
    }
    Iterator.single(context)
  }
}
