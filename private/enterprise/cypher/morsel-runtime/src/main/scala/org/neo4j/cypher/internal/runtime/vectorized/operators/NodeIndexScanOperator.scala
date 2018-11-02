/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.{IndexOrder, IndexReference, NodeValueIndexCursor}


class NodeIndexScanOperator(nodeOffset: Int,
                            label: Int,
                            property: SlottedIndexedProperty,
                            argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, property.maybeCachedNodePropertySlot) {

  override def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext): ContinuableOperatorTask = {
    val valueIndexCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()
    val index = context.transactionalContext.schemaRead.index(label, property.propertyKeyId)
    new OTask(valueIndexCursor, index)
  }

  class OTask(valueIndexCursor: NodeValueIndexCursor, index: IndexReference) extends ContinuableOperatorTask {

    var hasMore = false
    override def operate(currentRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {

      val read = context.transactionalContext.dataRead

      if (!hasMore) {
        read.nodeIndexScan(index, valueIndexCursor, IndexOrder.NONE, property.maybeCachedNodePropertySlot.isDefined)
      }

      hasMore = iterate(currentRow, valueIndexCursor, argumentSize)
    }

    override def canContinue: Boolean = hasMore
  }
}
