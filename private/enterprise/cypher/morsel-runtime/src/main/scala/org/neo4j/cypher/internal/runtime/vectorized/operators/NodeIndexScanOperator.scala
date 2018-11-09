/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.{IndexOrder, IndexReadSession, NodeValueIndexCursor}


class NodeIndexScanOperator(nodeOffset: Int,
                            label: Int,
                            property: SlottedIndexedProperty,
                            argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, property.maybeCachedNodePropertySlot) {

  override def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext, cursors: ExpressionCursors): ContinuableOperatorTask = {
    val valueIndexCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()
    val indexReference = context.transactionalContext.schemaRead.index(label, property.propertyKeyId)
    val indexSession = context.transactionalContext.dataRead.getOrCreateIndexReadSession(indexReference)
    new OTask(valueIndexCursor, indexSession)
  }

  class OTask(valueIndexCursor: NodeValueIndexCursor, index: IndexReadSession) extends ContinuableOperatorTask {

    var hasMore = false
    override def operate(currentRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit = {

      val read = context.transactionalContext.dataRead

      if (!hasMore) {
        read.nodeIndexScan(index, valueIndexCursor, IndexOrder.NONE, property.maybeCachedNodePropertySlot.isDefined)
      }

      hasMore = iterate(currentRow, valueIndexCursor, argumentSize)
    }

    override def canContinue: Boolean = hasMore
  }
}
