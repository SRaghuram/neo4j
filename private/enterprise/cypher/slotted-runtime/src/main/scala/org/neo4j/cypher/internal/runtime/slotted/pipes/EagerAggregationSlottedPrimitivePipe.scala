/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util

import org.neo4j.cypher.internal.physical_planning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.{LongArray, Values}

import scala.collection.JavaConverters._

//  This is a pipe can be used when the grouping is on all primitive long columns.
case class EagerAggregationSlottedPrimitivePipe(source: Pipe,
                                                slots: SlotConfiguration,
                                                readGrouping: Array[Int], // Offsets into the long array of the current execution context
                                                writeGrouping: Array[Int], // Offsets into the long array of the current execution context
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

    val result = new util.LinkedHashMap[LongArray, Seq[AggregationFunction]]()

    def createResultRow(groupingKey: LongArray, aggregator: Seq[AggregationFunction]): ExecutionContext = {
      val context = SlottedExecutionContext(slots)
      setKeyToCtx(context, groupingKey)
      (aggregationOffsets zip aggregator.map(_.result(state))).foreach {
        case (offset, value) => context.setRefAt(offset, value)
      }
      context
    }

    def setKeyFromCtx(ctx: ExecutionContext): LongArray = {
      val keys = new Array[Long](readGrouping.length)
      var i = 0
      while (i < readGrouping.length) {
        keys(i) = ctx.getLongAt(readGrouping(i))
        i += 1
      }
      Values.longArray(keys)
    }

    def setKeyToCtx(ctx: ExecutionContext, key: LongArray): Unit = {
      var i = 0
      while (i < writeGrouping.length) {
        ctx.setLongAt(writeGrouping(i), key.longValue(i))
        i += 1
      }
    }

    val createAggregationFunctions = new java.util.function.Function[LongArray, Seq[AggregationFunction]] {
      override def apply(t: LongArray): Seq[AggregationFunction] = aggregationFunctions.map(_.createAggregationFunction)
    }

    // Consume all input and aggregate
    input.foreach(ctx => {
      val keys = setKeyFromCtx(ctx)
      val aggregationFunctions = result.computeIfAbsent(keys, createAggregationFunctions)
      aggregationFunctions.foreach(func => func(ctx, state))
    })

    // Write the produced aggregation map to the output pipeline
    result.entrySet().iterator().asScala.map {
      e: java.util.Map.Entry[LongArray, Seq[AggregationFunction]] => createResultRow(e.getKey, e.getValue)
    }
  }
}
