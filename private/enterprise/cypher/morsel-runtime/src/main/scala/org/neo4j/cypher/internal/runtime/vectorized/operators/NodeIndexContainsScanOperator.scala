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
import org.opencypher.v9_0.util.CypherTypeException

class NodeIndexContainsScanOperator(nodeOffset: Int,
                                    label: Int,
                                    property: SlottedIndexedProperty,
                                    valueExpr: Expression,
                                    argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, property.maybeCachedNodePropertySlot) {

  override def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext, cursors: ExpressionCursors): ContinuableOperatorTask = {
    val valueIndexCursor: NodeValueIndexCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()
    val index = context.transactionalContext.schemaRead.index(label, property.propertyKeyId)
    new OTask(valueIndexCursor, index)
  }

  class OTask(valueIndexCursor: NodeValueIndexCursor, index: IndexReference) extends ContinuableOperatorTask {

    var hasMore = false
    override def operate(currentRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit = {

      val read = context.transactionalContext.dataRead

      var nullExpression: Boolean = false

      if (!hasMore) {
        val queryState = new OldQueryState(context, resources = null, params = state.params, cursors, Array.empty[IndexReadSession])
        val value = valueExpr(currentRow, queryState)

        value match {
          case value: TextValue =>
            read.nodeIndexSeek(index,
                               valueIndexCursor,
                               IndexOrder.NONE,
                               property.maybeCachedNodePropertySlot.isDefined,
                               IndexQuery.stringContains(index.properties()(0), value.stringValue()))

          case Values.NO_VALUE =>
            // CONTAINS null does not produce any rows
            nullExpression = true

          case x => throw new CypherTypeException(s"Expected a string value, but got $x")
        }
      }

      if (!nullExpression)
        hasMore = iterate(currentRow, valueIndexCursor, argumentSize)
      else
        hasMore = false
    }

    override def canContinue: Boolean = hasMore
  }
}
