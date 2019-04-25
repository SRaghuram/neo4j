/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.function

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it can't - this pipe will eagerly load the full match
case class EagerAggregationSlottedPipe(source: Pipe,
                                       slots: SlotConfiguration,
                                       groupingExpression: GroupingExpression,
                                       aggregations: Map[Int, AggregationExpression])
                                      (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))
  groupingExpression.registerOwningPipe(this)

  private val (aggregationOffsets: IndexedSeq[Int], aggregationFunctions: IndexedSeq[AggregationExpression]) = {
    val (a,b) = aggregations.unzip
    (a.toIndexedSeq, b.toIndexedSeq)
  }
  private val computeAggregations: function.Function[ groupingExpression.KeyType,Seq[AggregationFunction]] =
    (_: groupingExpression.KeyType) => aggregationFunctions.map(_.createAggregationFunction)

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    val result = new java.util.LinkedHashMap[groupingExpression.KeyType, Seq[AggregationFunction]]()

    // Used when we have no input and no grouping expressions. In this case, we'll return a single row
    def createEmptyResult(): Iterator[ExecutionContext] = {
      val context = SlottedExecutionContext(slots)
      val aggregationOffsetsAndFunctions = aggregationOffsets zip aggregations
        .map(_._2.createAggregationFunction.result(state))

      aggregationOffsetsAndFunctions.toMap.foreach {
        case (offset, zeroValue) => context.setRefAt(offset, zeroValue)
      }
      Iterator.single(context)
    }

    def writeAggregationResultToContext(groupingKey: groupingExpression.KeyType, aggregator: Seq[AggregationFunction]): ExecutionContext = {
      val context = SlottedExecutionContext(slots)
      groupingExpression.project(context, groupingKey)
      (aggregationOffsets zip aggregator.map(_.result(state))).foreach {
        case (offset, value) => context.setRefAt(offset, value)
      }
      context
    }

    // Consume all input and aggregate
    input.foreach(ctx => {
      val groupingValue = groupingExpression.computeGroupingKey(ctx, state)
      val functions = result.computeIfAbsent(groupingValue, computeAggregations)
      functions.foreach(func => func(ctx, state))
    })

    // Write the produced aggregation map to the output pipeline
    if (result.isEmpty && groupingExpression.isEmpty) {
      createEmptyResult()
    } else {
      val iterator = result.entrySet().iterator()
      new Iterator[ExecutionContext] {
        override def hasNext: Boolean = iterator.hasNext
        override def next(): ExecutionContext = {
          val entry = iterator.next()
          writeAggregationResultToContext(entry.getKey, entry.getValue)
        }
      }
    }
  }
}
