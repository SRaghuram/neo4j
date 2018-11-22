/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api._
import org.neo4j.values.storable.{TextValue, Values}
import org.neo4j.cypher.internal.v4_0.util.CypherTypeException

class NodeIndexContainsScanOperator(nodeOffset: Int,
                                    label: Int,
                                    property: SlottedIndexedProperty,
                                    valueExpr: Expression,
                                    argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, property.maybeCachedNodePropertySlot) {

  override def init(context: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    cursors: ExpressionCursors): ContinuableOperatorTask = {
    val index = context.transactionalContext.schemaRead.index(label, property.propertyKeyId)
    val indexSession = context.transactionalContext.dataRead.indexReadSession(index)
    new OTask(inputMorsel, indexSession)
  }

  class OTask(val inputRow: MorselExecutionContext, index: IndexReadSession) extends StreamingContinuableOperatorTask {

    private var valueIndexCursor: NodeValueIndexCursor = _

    override def initializeInnerLoop(inputRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): AutoCloseable = {
      valueIndexCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()

      val read = context.transactionalContext.dataRead

      var nullExpression: Boolean = false

      val queryState = new OldQueryState(context, resources = null, params = state.params, cursors, Array.empty[IndexReadSession])
      val value = valueExpr(inputRow, queryState)

      value match {
        case value: TextValue =>
          val indexQuery = IndexQuery.stringContains(property.propertyKeyId, value)
          read.nodeIndexSeek(index, valueIndexCursor, IndexOrder.NONE, property.maybeCachedNodePropertySlot.isDefined, indexQuery)

        case Values.NO_VALUE =>
          // CONTAINS null does not produce any rows
          nullExpression = true

        case x => throw new CypherTypeException(s"Expected a string value, but got $x")
      }

      if (nullExpression)
        null
      else
        valueIndexCursor
    }

    override def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputRow, outputRow, valueIndexCursor, argumentSize)
    }
  }
}
