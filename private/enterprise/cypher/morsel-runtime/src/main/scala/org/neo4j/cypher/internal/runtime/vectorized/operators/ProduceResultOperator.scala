/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import java.util

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.LongSlot
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.RefSlot
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.AnyValue
import org.opencypher.v9_0.util.symbols

class ProduceResultOperator(slots: SlotConfiguration, fieldNames: Array[String]) extends LazyReduceOperator {

  override def init(context: QueryContext,
                    state: QueryState,
                    messageQueue: util.Queue[MorselExecutionContext],
                    collector: LazyReduceCollector,
                    cursors: ExpressionCursors): ContinuableOperatorTask = new OTask(messageQueue, collector)

  class OTask(messageQueue: util.Queue[MorselExecutionContext], collector: LazyReduceCollector) extends LazyReduceOperatorTask(messageQueue, collector) {

    override def operateSingleMorsel(context: QueryContext,
                                     state: QueryState,
                                     currentRow: MorselExecutionContext): Unit = {
      val resultRow = new MorselResultRow(currentRow, slots, fieldNames, context)
      // Loop over the rows of the morsel and call the visitor for each one
      while (currentRow.hasMoreRows) {
        state.visitor.visit(resultRow)
        currentRow.moveToNextRow()
      }
    }
  }
}

class MorselResultRow(currentRow: MorselExecutionContext,
                      slots: SlotConfiguration,
                      fieldNames: Array[String],
                      queryContext: QueryContext) extends QueryResult.Record {
  private val array = new Array[AnyValue](fieldNames.length)

  private val updateArray: Array[() => AnyValue] = fieldNames.map(key => slots.get(key) match {
    case None => throw new IllegalStateException()
    case Some(RefSlot(offset, _, _)) => () =>
      currentRow.getRefAt(offset)
    case Some(LongSlot(offset, _, symbols.CTNode)) => () =>
      val nodeId = currentRow.getLongAt(offset)
      queryContext.nodeOps.getById(nodeId)
    case Some(LongSlot(offset, _, symbols.CTRelationship)) => () =>
      val relationshipId = currentRow.getLongAt(offset)
      queryContext.relationshipOps.getById(relationshipId)
    case _ => throw new IllegalStateException
  })

  override def fields(): Array[AnyValue] = {
    var i = 0
    while ( i < array.length) {
      array(i) = updateArray(i)()
      i += 1
    }
    array
  }
}
