/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{AggregationExpression, Expression}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, MapValue, VirtualValues}
import org.opencypher.v9_0.util.attribution.Id

import scala.collection.{immutable, mutable}

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it can't - this pipe will eagerly load the full match
case class EagerAggregationSlottedPipe(source: Pipe,
                                       slots: SlotConfiguration,
                                       groupingExpressions: Map[Slot, Expression],
                                       aggregations: Map[Int, AggregationExpression])
                                      (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))
  groupingExpressions.values.foreach(_.registerOwningPipe(this))

  private val (aggregationOffsets: IndexedSeq[Int], aggregationFunctions: IndexedSeq[AggregationExpression]) = {
    val (a,b) = aggregations.unzip
    (a.toIndexedSeq, b.toIndexedSeq)
  }

  private val expressionOrder: immutable.Seq[(Slot, Expression)] = groupingExpressions.toIndexedSeq

  private val groupingFunction: (ExecutionContext, QueryState) => AnyValue = {
    groupingExpressions.size match {
      case 1 =>
        val firstExpression = groupingExpressions.head._2
        (ctx, state) =>
          firstExpression(ctx, state)

      case 2 =>
        val e1 = groupingExpressions.head._2
        val e2 = groupingExpressions.last._2
        (ctx, state) => VirtualValues.list(e1(ctx, state), e2(ctx, state))

      case 3 =>
        val e1 = groupingExpressions.head._2
        val e2 = groupingExpressions.tail.head._2
        val e3 = groupingExpressions.last._2
        (ctx, state) => VirtualValues.list(e1(ctx, state), e2(ctx, state), e3(ctx, state))

      case _ =>
        val expressions = groupingExpressions.values.toSeq
        (ctx, state) => VirtualValues.list(expressions.map(e => e(ctx, state)): _*)
    }
  }

  // This is assigned a specialized update function at compile time
  private val addGroupingValuesToResult: (ExecutionContext, AnyValue) => Unit = {
    val setInSlotFunctions = expressionOrder.map {
      case (slot, _) =>
        SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor(slot)
    }
    val unusedSetInSlotFunction = (context: ExecutionContext, groupingKey: AnyValue) => ???
    val setInSlotFunction1 = if (groupingExpressions.size >= 1) setInSlotFunctions(0) else unusedSetInSlotFunction
    val setInSlotFunction2 = if (groupingExpressions.size >= 2) setInSlotFunctions(1) else unusedSetInSlotFunction
    val setInSlotFunction3 = if (groupingExpressions.size >= 3) setInSlotFunctions(2) else unusedSetInSlotFunction

    groupingExpressions.size match {
      case 1 =>
        setInSlotFunction1
      case 2 =>
        (context: ExecutionContext, groupingKey: AnyValue) => {
          val t2 = groupingKey.asInstanceOf[ListValue]
          setInSlotFunction1(context, t2.head())
          setInSlotFunction2(context, t2.last())
        }
      case 3 =>
        (context: ExecutionContext, groupingKey: AnyValue) => {
          val t3 = groupingKey.asInstanceOf[ListValue]
          setInSlotFunction1(context, t3.value(0))
          setInSlotFunction2(context, t3.value(1))
          setInSlotFunction3(context, t3.value(2))
        }
      case _ =>
        (context: ExecutionContext, groupingKey: AnyValue) => {
          val listOfValues = groupingKey.asInstanceOf[ListValue]
          for (i <- 0 until groupingExpressions.size) {
            val value: AnyValue = listOfValues.value(i)
            setInSlotFunctions(i)(context, value)
          }
        }
    }
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    val result = mutable.LinkedHashMap[AnyValue, Seq[AggregationFunction]]()

    // Used when we have no input and no grouping expressions. In this case, we'll return a single row
    def createEmptyResult(params: MapValue): Iterator[ExecutionContext] = {
      val context = executionContextFactory.newExecutionContext()
      val aggregationOffsetsAndFunctions = aggregationOffsets zip aggregations
        .map(_._2.createAggregationFunction.result(state))

      aggregationOffsetsAndFunctions.toMap.foreach {
        case (offset, zeroValue) => context.setRefAt(offset, zeroValue)
      }
      Iterator.single(context)
    }

    def writeAggregationResultToContext(groupingKey: AnyValue, aggregator: Seq[AggregationFunction]): ExecutionContext = {
      val context = executionContextFactory.newExecutionContext()
      addGroupingValuesToResult(context, groupingKey)
      (aggregationOffsets zip aggregator.map(_.result(state))).foreach {
        case (offset, value) => context.setRefAt(offset, value)
      }
      context
    }

    // Consume all input and aggregate
    input.foreach(ctx => {
      val groupingValue: AnyValue = groupingFunction(ctx, state)
      val functions = result.getOrElseUpdate(groupingValue, aggregationFunctions.map(_.createAggregationFunction))
      functions.foreach(func => func(ctx, state))
    })

    // Write the produced aggregation map to the output pipeline
    if (result.isEmpty && groupingExpressions.isEmpty) {
      createEmptyResult(state.params)
    } else {
      result.map {
        case (key, aggregator) => writeAggregationResultToContext(key, aggregator)
      }.toIterator
    }
  }
}
