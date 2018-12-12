/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.{IndexOrder, IndexReadSession, NodeValueIndexCursor}


class NodeIndexScanOperator(val workIdentity: WorkIdentity,
                            nodeOffset: Int,
                            label: Int,
                            property: SlottedIndexedProperty,
                            queryIndexId: Int,
                            argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, property.maybeCachedNodePropertySlot) {

  override def init(context: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    resources: QueryResources): ContinuableOperatorTask = {
    val indexSession = state.queryIndexes(queryIndexId)
    new OTask(inputMorsel, indexSession)
  }

  class OTask(val inputRow: MorselExecutionContext, index: IndexReadSession) extends StreamingContinuableOperatorTask {
    var valueIndexCursor: NodeValueIndexCursor = _

    protected override def initializeInnerLoop(inputRow: MorselExecutionContext,
                                               context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): AutoCloseable = {
      valueIndexCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()
      val read = context.transactionalContext.dataRead
      read.nodeIndexScan(index, valueIndexCursor, IndexOrder.NONE, property.maybeCachedNodePropertySlot.isDefined)
      valueIndexCursor
    }

    override def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputRow, outputRow, valueIndexCursor, argumentSize)
    }
  }
}
