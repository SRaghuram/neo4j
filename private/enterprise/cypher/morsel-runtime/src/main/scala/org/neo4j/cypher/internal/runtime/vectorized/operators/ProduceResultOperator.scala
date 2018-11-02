/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.AnyValue
import org.opencypher.v9_0.util.symbols


class ProduceResultOperator(slots: SlotConfiguration, fieldNames: Array[String]) extends StatelessOperator {

  override def operate(currentRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
    val resultRow = new MorselResultRow(currentRow, slots, fieldNames, context)

    while (currentRow.hasMoreRows) {
      state.visitor.visit(resultRow)
      currentRow.moveToNextRow()
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
